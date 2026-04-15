import logging
from datetime import datetime
from typing import Any, Iterable, List, Optional, Tuple

import base58

from infra.parser_base import ProtocolParser
from core.types import PoolCreatedEvent, SwapEvent


logger = logging.getLogger(__name__)


def _get_account_keys(raw_tx: dict) -> List[str]:
    """
    Полный список публичных ключей транзакции.
    Для versioned tx с Address Lookup Tables: message.accountKeys + meta.loadedAddresses.
    """
    msg = raw_tx.get("message") or raw_tx.get("transaction", {}).get("message")
    if not msg:
        return []
    keys = msg.get("accountKeys")
    if keys is None:
        return []
    out: List[str] = []
    for k in keys:
        if isinstance(k, str):
            out.append(k)
        elif isinstance(k, dict) and "pubkey" in k:
            out.append(k["pubkey"])
        else:
            out.append("")

    meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
    loaded = meta.get("loadedAddresses") or {}
    for key in loaded.get("writable") or []:
        out.append(key if isinstance(key, str) else "")
    for key in loaded.get("readonly") or []:
        out.append(key if isinstance(key, str) else "")
    return out


def _get_all_instructions(raw_tx: dict) -> List[Tuple[Any, dict, Optional[int]]]:
    """
    Вернуть список (program_id_index или program_id строка, instruction, parent_index).
    parent_index = None для внешних инструкций.
    """
    msg = raw_tx.get("message") or raw_tx.get("transaction", {}).get("message")
    if not msg:
        return []
    instructions = msg.get("instructions") or []
    result: List[Tuple[Any, dict, Optional[int]]] = []
    for ix in instructions:
        result.append((ix.get("programIdIndex", ix.get("programId")), ix, None))

    meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
    inner = meta.get("innerInstructions") or []
    for inner_block in inner:
        parent_idx = inner_block.get("index")
        for ix in inner_block.get("instructions", []):
            result.append((ix.get("programIdIndex", ix.get("programId")), ix, parent_idx))
    return result


def _decode_instruction_data(instruction: dict) -> bytes:
    data = instruction.get("data")
    if data is None:
        return b""
    if isinstance(data, str):
        # В jsonParsed RPC Solana инструкции Anchor часто кодируются в base58.
        try:
            return base58.b58decode(data)
        except Exception:
            return b""
    if isinstance(data, list):
        return bytes(data)
    return b""


def _resolve_account(account_keys: List[str], instruction: dict, index: int) -> str:
    """
    Вернуть pubkey по индексу в списке аккаунтов инструкции.
    accounts может быть: массив индексов (int) в accountKeys или массив pubkey (str) в jsonParsed.
    """
    accounts = instruction.get("accounts") or []
    if index < 0 or index >= len(accounts):
        return ""
    acc = accounts[index]
    if isinstance(acc, str):
        return acc
    if isinstance(acc, int):
        if acc < 0 or acc >= len(account_keys):
            return ""
        return account_keys[acc]
    return ""


def _timestamp_from_tx(raw_tx: dict) -> datetime:
    block_time = raw_tx.get("blockTime") or (raw_tx.get("transaction") or {}).get("blockTime")
    if block_time is not None:
        return datetime.utcfromtimestamp(int(block_time))
    return datetime.utcnow()


def _signature_from_tx(raw_tx: dict) -> str:
    tx = raw_tx.get("transaction") or raw_tx
    sigs = tx.get("signatures")
    if sigs and len(sigs) > 0:
        return sigs[0] if isinstance(sigs[0], str) else ""
    return ""


def _slot_from_tx(raw_tx: dict) -> int:
    return int(raw_tx.get("slot") or 0)


# -----------------------------
# IDL‑карты по дискриминаторам
# -----------------------------

# create_pool
_INIT_POOL_BY_DISC: dict[bytes, dict[str, int]] = {
    # discriminator: [233,146,209,142,207,104,64,188]
    bytes([233, 146, 209, 142, 207, 104, 64, 188]): {
        "pool_index": 0,        # pool
        "base_mint_index": 3,   # base_mint
        "quote_mint_index": 4,  # quote_mint
    }
}

# buy / buy_exact_quote_in / sell
_SWAP_BY_DISC: dict[bytes, dict[str, int]] = {
    # buy
    bytes([102, 6, 61, 18, 1, 218, 235, 234]): {
        "pool_index": 0,  # pool
    },
    # buy_exact_quote_in
    bytes([198, 46, 21, 82, 180, 217, 232, 112]): {
        "pool_index": 0,
    },
    # sell
    bytes([51, 230, 133, 164, 1, 127, 131, 173]): {
        "pool_index": 0,
    },
}


# Известные quote‑токены (как в Meteora).
KNOWN_QUOTE_MINTS: set[str] = {
    # WSOL
    "So11111111111111111111111111111111111111112",
    # Основные стейблы (mainnet)
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
}


class PumpSwapParser(ProtocolParser):
    """
    Парсер PumpSwap по Anchor IDL (`protocols/pumpswap_idl.json`).

    - Пулы: по инструкции `create_pool`.
    - Свапы: по инструкциям `buy` / `buy_exact_quote_in` / `sell`,
      считаем любые сделки с участием WSOL >= 4 SOL.
    """

    @property
    def protocol_name(self) -> str:
        return "pumpswap"

    def __init__(self, program_id: str, min_sol_lamports: int = 10_000_000) -> None:
        self.program_id = program_id
        # Минимальная сумма по SOL (в лампортах) для учёта свапа.
        # По умолчанию 0.01 SOL, но значение можно переопределить через конфигурацию.
        self._min_sol_lamports = min_sol_lamports

    # -----------------------------
    # Вспомогательные методы
    # -----------------------------

    def _resolve_program_id(self, raw_tx: dict, program_id_ref: Any) -> bool:
        if program_id_ref is None:
            return False
        account_keys = _get_account_keys(raw_tx)
        if isinstance(program_id_ref, str):
            return program_id_ref == self.program_id
        if isinstance(program_id_ref, int):
            if program_id_ref < 0 or program_id_ref >= len(account_keys):
                return False
            return account_keys[program_id_ref] == self.program_id
        return False

    # -----------------------------
    # Интерфейс ProtocolParser
    # -----------------------------

    def supports(self, raw_tx: dict) -> bool:
        account_keys = _get_account_keys(raw_tx)
        if self.program_id in account_keys:
            return True
        return any(
            self._resolve_program_id(raw_tx, prog_ref)
            for prog_ref, _, _ in _get_all_instructions(raw_tx)
        )

    def parse_pool_creations(self, raw_tx: dict) -> Iterable[PoolCreatedEvent]:
        account_keys = _get_account_keys(raw_tx)
        sig = _signature_from_tx(raw_tx)
        slot = _slot_from_tx(raw_tx)
        ts = _timestamp_from_tx(raw_tx)

        seen_pool_for_tx = False

        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            if seen_pool_for_tx:
                continue

            data = _decode_instruction_data(ix)
            disc = data[:8] if len(data) >= 8 else b""
            mapping = _INIT_POOL_BY_DISC.get(disc)
            if mapping is None:
                continue

            pool_idx = mapping["pool_index"]
            base_idx = mapping["base_mint_index"]
            quote_idx = mapping["quote_mint_index"]

            pool_addr = _resolve_account(account_keys, ix, pool_idx)
            base_mint = _resolve_account(account_keys, ix, base_idx)
            quote_mint = _resolve_account(account_keys, ix, quote_idx)

            if not pool_addr or not base_mint or not quote_mint:
                logger.debug(
                    "[PUMPSWAP-IDL] slot=%s tx=%s disc=%s mapping есть, но аккаунты пустые, пропускаем",
                    slot,
                    sig,
                    list(disc),
                )
                continue

            # base_mint должен быть токен пула, quote_mint — SOL/стейбл. Если по IDL в base попал quote (WSOL) — меняем местами.
            if base_mint in KNOWN_QUOTE_MINTS and quote_mint not in KNOWN_QUOTE_MINTS:
                base_mint, quote_mint = quote_mint, base_mint

            seen_pool_for_tx = True

            yield PoolCreatedEvent(
                protocol="pumpswap",
                pool_address=pool_addr,
                base_mint=base_mint,
                quote_mint=quote_mint,
                tx_signature=sig,
                slot=slot,
                timestamp=ts,
                extra={},
            )

    def parse_swaps(self, raw_tx: dict) -> Iterable[SwapEvent]:
        """
        Ищем любые свапы PumpSwap по дискриминаторам из IDL.
        Учитываем все сделки с участием SOL (WSOL) на сумму >= 4 SOL.
        """
        if not _SWAP_BY_DISC:
            return

        account_keys = _get_account_keys(raw_tx)
        signer: Optional[str] = account_keys[0] if account_keys else None
        sig = _signature_from_tx(raw_tx)
        slot = _slot_from_tx(raw_tx)
        ts = _timestamp_from_tx(raw_tx)

        # Находим первую swap‑инструкцию PumpSwap и определяем pool.
        pool_addr: Optional[str] = None
        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            data = _decode_instruction_data(ix)
            disc = data[:8] if len(data) >= 8 else b""
            mapping = _SWAP_BY_DISC.get(disc)
            if mapping is None:
                continue
            pool_idx = mapping["pool_index"]
            pool_addr = _resolve_account(account_keys, ix, pool_idx)
            logger.debug(
                "[PUMPSWAP-SWAP-MATCH] tx=%s slot=%s disc=%s pool=%s",
                sig,
                slot,
                list(disc),
                pool_addr,
            )
            break

        # Без пула корректно распознать своп не сможем.
        if not pool_addr:
            return

        # Собираем дельты балансов по (owner, mint).
        meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
        pre = meta.get("preTokenBalances") or []
        post = meta.get("postTokenBalances") or []

        def _to_map(entries: list[dict]) -> dict[tuple[int, str, str], tuple[int, int]]:
            m: dict[tuple[int, str, str], tuple[int, int]] = {}
            for e in entries:
                if not isinstance(e, dict):
                    continue
                idx = e.get("accountIndex")
                mint = e.get("mint")
                owner = e.get("owner")
                ui = e.get("uiTokenAmount") or {}
                amount = ui.get("amount")
                decimals = ui.get("decimals")
                if not isinstance(idx, int) or not isinstance(mint, str) or not isinstance(owner, str):
                    continue
                try:
                    amt_int = int(amount)
                except Exception:
                    continue
                if not isinstance(decimals, int):
                    decimals = 0
                m[(idx, mint, owner)] = (amt_int, decimals)
            return m

        pre_map = _to_map(pre)
        post_map = _to_map(post)

        # Дельты по (owner, mint): post - pre.
        owner_mint_delta: dict[tuple[str, str], tuple[int, int]] = {}
        keys = set(pre_map.keys()) | set(post_map.keys())
        for k in keys:
            pre_amt, pre_dec = pre_map.get(k, (0, 0))
            post_amt, post_dec = post_map.get(k, (0, 0))
            decimals = post_dec or pre_dec
            delta = post_amt - pre_amt
            _, mint, owner = k
            om_key = (owner, mint)
            cur = owner_mint_delta.get(om_key, (0, decimals))
            owner_mint_delta[om_key] = (cur[0] + delta, decimals)

        # Ищем любые свапы с участием SOL (WSOL) для любых владельцев.
        # Минимум по SOL только отсекает пыль; основной фильтр — по amount_usd в раннере.
        MIN_SOL_LAMPORTS = self._min_sol_lamports
        swaps: list[tuple[str, str, int, str, int]] = []  # (owner, in_mint, in_amt, out_mint, out_amt)

        WSOL_MINT = "So11111111111111111111111111111111111111112"

        for (owner, mint), (delta, decimals) in owner_mint_delta.items():
            if mint != WSOL_MINT or delta == 0:
                continue

            abs_delta = -delta if delta < 0 else delta
            logger.debug(
                "[PUMPSWAP-SWAP-CANDIDATE] tx=%s owner=%s wsol_delta=%s lamports (min=%s)",
                sig,
                owner,
                delta,
                MIN_SOL_LAMPORTS,
            )
            if abs_delta < MIN_SOL_LAMPORTS:
                continue

            # delta < 0  ⇒ владелец потратил WSOL и получил другой токен (покупка токена за SOL).
            # delta > 0  ⇒ владелец получил WSOL и отдал другой токен  (продажа токена за SOL).
            if delta < 0:
                in_mint = WSOL_MINT
                in_amt = abs_delta
                # Ищем среди токенов этого же owner'а рост баланса (покупка токена).
                for (owner2, mint2), (delta2, _) in owner_mint_delta.items():
                    if owner2 != owner or mint2 == mint or delta2 <= 0:
                        continue
                    swaps.append((owner, in_mint, in_amt, mint2, delta2))
                    break
            else:
                out_mint = WSOL_MINT
                out_amt = abs_delta
                # Ищем среди токенов этого же owner'а падение баланса (продажа токена за SOL).
                for (owner2, mint2), (delta2, _) in owner_mint_delta.items():
                    if owner2 != owner or mint2 == mint or delta2 >= 0:
                        continue
                    in_mint = mint2
                    in_amt = -delta2
                    swaps.append((owner, in_mint, in_amt, out_mint, out_amt))
                    break

        if not swaps:
            return

        # Эмитим SwapEvent для каждого подходящего swap‑кейса.
        if swaps:
            logger.debug(
                "[PUMPSWAP-SWAP] tx=%s slot=%s pool=%s swaps=%s",
                sig,
                slot,
                pool_addr,
                swaps,
            )
        for owner, in_mint, in_amt, out_mint, out_amt in swaps:
            # Исправление: в коде выше in = отдаёт, out = получает; в БД ожидаем наоборот — меняем местами.
            yield SwapEvent(
                protocol="pumpswap",
                pool_address=pool_addr or "",
                in_mint=out_mint,
                out_mint=in_mint,
                in_amount=out_amt,
                out_amount=in_amt,
                
                trader=signer or owner,
                tx_signature=sig,
                slot=slot,
                timestamp=ts,
            )

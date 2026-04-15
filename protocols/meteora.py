import json
import logging
from datetime import datetime
from pathlib import Path
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


def _resolve_account(account_keys: List[str], instruction: dict, index: int) -> str:
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


def _decode_instruction_data(instruction: dict) -> bytes:
    data = instruction.get("data")
    if data is None:
        return b""
    if isinstance(data, str):
        # В jsonParsed RPC Solana инструкции кодируются в base58, а не в base64.
        # Используем base58.b58decode, чтобы корректно получить Anchor‑дискриминатор.
        try:
            return base58.b58decode(data)
        except Exception:
            return b""
    if isinstance(data, list):
        return bytes(data)
    return b""


def _mint_decimals_map(raw_tx: dict) -> dict[str, int]:
    """
    Собираем карту mint -> decimals из meta.postTokenBalances.
    Это помогает отличать NFT (обычно decimals=0) от обычных токенов (6/9 и т.п.).
    """
    meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
    balances = meta.get("postTokenBalances") or []
    out: dict[str, int] = {}
    for b in balances:
        if not isinstance(b, dict):
            continue
        mint = b.get("mint")
        ui = b.get("uiTokenAmount") or {}
        if not isinstance(mint, str):
            continue
        decimals = ui.get("decimals")
        if isinstance(decimals, int):
            out[mint] = decimals
    return out


#
# Загрузка IDL Meteora и подготовка мапы:
#   discriminator(8 байт) -> индексы аккаунтов pool / token_a_mint / token_b_mint.
#

_IDL_PATH = Path(__file__).with_name("meteora_idl.json")


def _load_meteora_idl() -> dict | None:
    try:
        with _IDL_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _build_init_pool_mapping(idl: dict | None) -> dict[bytes, dict[str, int]]:
    mapping: dict[bytes, dict[str, int]] = {}
    if not idl:
        return mapping

    for inst in idl.get("instructions", []):
        name = str(inst.get("name", "")).lower()
        # Нас интересуют только Initialize*Pool* инструкции.
        if "initialize" not in name or "pool" not in name:
            continue
        discr_list = inst.get("discriminator")
        if not isinstance(discr_list, list) or len(discr_list) != 8:
            continue
        try:
            disc_bytes = bytes(int(x) & 0xFF for x in discr_list)
        except Exception:
            continue

        accounts = inst.get("accounts") or []
        index_by_name: dict[str, int] = {}
        for idx, acc in enumerate(accounts):
            acc_name = acc.get("name")
            if isinstance(acc_name, str):
                index_by_name[acc_name] = idx

        # В большинстве инструкций имена примерно такие:
        #   pool, token_a_mint, token_b_mint
        pool_idx = index_by_name.get("pool")
        token_a_idx = index_by_name.get("token_a_mint") or index_by_name.get("tokenAMint")
        token_b_idx = index_by_name.get("token_b_mint") or index_by_name.get("tokenBMint")

        if pool_idx is None or token_a_idx is None or token_b_idx is None:
            continue

        mapping[disc_bytes] = {
            "pool_index": pool_idx,
            "token_a_mint_index": token_a_idx,
            "token_b_mint_index": token_b_idx,
        }

    return mapping


def _build_swap_mapping(idl: dict | None) -> dict[bytes, dict[str, int]]:
    """
    Сопоставление дискриминаторам swap‑инструкций индексов ключевых аккаунтов.
    """
    mapping: dict[bytes, dict[str, int]] = {}
    if not idl:
        return mapping

    for inst in idl.get("instructions", []):
        name = str(inst.get("name", "")).lower()
        # Берём только swap‑инструкции (без уточнений, IDL уже отфильтрован по протоколу).
        if "swap" not in name:
            continue

        discr_list = inst.get("discriminator")
        if not isinstance(discr_list, list) or len(discr_list) != 8:
            continue
        try:
            disc_bytes = bytes(int(x) & 0xFF for x in discr_list)
        except Exception:
            continue

        accounts = inst.get("accounts") or []
        index_by_name: dict[str, int] = {}
        for idx, acc in enumerate(accounts):
            acc_name = acc.get("name")
            if isinstance(acc_name, str):
                index_by_name[acc_name] = idx

        pool_idx = index_by_name.get("pool")
        # В разных версиях IDL поле плательщика может называться по‑разному.
        payer_idx = (
            index_by_name.get("payer")
            or index_by_name.get("user")
            or index_by_name.get("owner")
        )
        if pool_idx is None or payer_idx is None:
            continue

        mapping[disc_bytes] = {
            "pool_index": pool_idx,
            "payer_index": payer_idx,
        }

    return mapping


_METEORA_IDL = _load_meteora_idl()
_INIT_POOL_BY_DISC = _build_init_pool_mapping(_METEORA_IDL)
_SWAP_BY_DISC = _build_swap_mapping(_METEORA_IDL)
logger.info(
    "[METEORA-IDL] загружено %s инструкций, init-пулы: %s дискриминаторов, свапы: %s дискриминаторов",
    len(_METEORA_IDL.get("instructions", [])) if _METEORA_IDL else 0,
    len(_INIT_POOL_BY_DISC),
    len(_SWAP_BY_DISC),
)


#
# Известные quote‑токены для Meteora: стейблы / SOL / WSOL.
# Для них мы считаем, что они стоят "со стороны quote", а в base идёт новый токен.
#
KNOWN_QUOTE_MINTS: set[str] = {
    # WSOL
    "So11111111111111111111111111111111111111112",
    # Основные стейблы (mainnet)
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
}


def _has_pool_init_in_logs(raw_tx: dict) -> bool:
    """
    По логам определяем, что это инициализация пула Meteora.

    Модуль meteora_pools_sdk содержит кучу инструкций Initialize*Pool*,
    в логах они обычно выглядят как:
      \"Program log: Instruction: InitializePermissionlessPool\"
      \"Program log: Instruction: Initialize...ConstantProductPool...\"
    """
    meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
    logs = meta.get("logMessages") or []
    for line in logs:
        if not isinstance(line, str):
            continue
        lower = line.lower()
        if "instruction:" in lower and "initialize" in lower and "pool" in lower:
            return True
    return False


class MeteoraParser(ProtocolParser):
    """
    Парсер пулов Meteora (constant product / permissionless pools и т.п.).

    Подход:
    - считаем транзакцию meteora‑овской, если в ней фигурирует program_id;
    - пул считаем созданным, если в логах есть Instruction: Initialize*Pool*;
    - для такой транзакции берём первую инструкцию программы Meteora и
      вытаскиваем из неё:
        accounts[0] — адрес пула (pool),
        accounts[2] — token_a_mint,
        accounts[3] — token_b_mint.
    """

    @property
    def protocol_name(self) -> str:
        return "meteora"

    def __init__(self, program_id: str, min_sol_lamports: int = 10_000_000) -> None:
        self.program_id = program_id
        # Минимальная сумма по SOL (в лампортах) для учёта свапа.
        # По умолчанию 0.01 SOL, но значение можно переопределить через конфигурацию.
        self._min_sol_lamports = min_sol_lamports

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
        mint_decimals = _mint_decimals_map(raw_tx)

        seen_pool_for_tx = False

        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            if seen_pool_for_tx:
                continue

            # 1) Пробуем вытащить pool / tokenA / tokenB по IDL‑дискриминатору.
            pool_addr: Optional[str] = None
            base_mint: Optional[str] = None
            quote_mint: Optional[str] = None

            data = _decode_instruction_data(ix)
            disc = data[:8] if len(data) >= 8 else b""

            # Логируем каждый дискриминатор, по которому пытаемся найти сопоставление.
            # LOG_LEVEL=DEBUG: подробный вывод, LOG_LEVEL=INFO: только случаи с маппингом.
            

            mapping = _INIT_POOL_BY_DISC.get(disc)

            if mapping is None:
                # Этот дискриминатор не относится к инициализации пула (например, свап) —
                # просто пропускаем инструкцию, не пытаясь угадывать по аккаунтам.
                continue

            logger.debug(
                "[METEORA-IDL] slot=%s tx=%s disc=%s matched mapping=%s",
                slot,
                sig,
                list(disc),
                mapping,
            )
            pool_idx = mapping["pool_index"]
            token_a_idx = mapping["token_a_mint_index"]
            token_b_idx = mapping["token_b_mint_index"]

            pool_addr = _resolve_account(account_keys, ix, pool_idx)
            token_a = _resolve_account(account_keys, ix, token_a_idx)
            token_b = _resolve_account(account_keys, ix, token_b_idx)

            if not pool_addr or not token_a or not token_b:
                # Если IDL не помог (например, странный аккаунт‑layout),
                # пропускаем инструкцию, чтобы не ловить ложные пулы.
                logger.debug(
                    "[METEORA-IDL] slot=%s tx=%s disc=%s mapping есть, но аккаунты пустые, "
                    "пропускаем инструкцию",
                    slot,
                    sig,
                    list(disc),
                )
                continue

            # К этому месту мы дошли только для инструкций init-пула по IDL.
            token_candidates = [token_a, token_b]

            if not pool_addr or len(token_candidates) < 2:
                continue

            # 3) Выбираем base/quote:
            # - quote: известный стейбл/WSOL
            # - base: не из KNOWN_QUOTE_MINTS и с decimals >= 5 (отсекаем NFT с decimals=0)
            for mint in token_candidates:
                if mint in KNOWN_QUOTE_MINTS:
                    quote_mint = mint
                else:
                    dec = mint_decimals.get(mint)
                    if dec is None or dec >= 5:
                        base_mint = mint

            # Если по эвристике не получилось, откатываемся к упрощённому выбору.
            if not base_mint or not quote_mint:
                token_a = token_candidates[0]
                token_b = token_candidates[1] if len(token_candidates) > 1 else token_candidates[0]
                if token_a in KNOWN_QUOTE_MINTS and token_b not in KNOWN_QUOTE_MINTS:
                    base_mint = token_b
                    quote_mint = token_a
                elif token_b in KNOWN_QUOTE_MINTS and token_a not in KNOWN_QUOTE_MINTS:
                    base_mint = token_a
                    quote_mint = token_b
                else:
                    base_mint = token_a
                    quote_mint = token_b

            seen_pool_for_tx = True

            yield PoolCreatedEvent(
                protocol="meteora",
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
        Ищем любые свапы Meteora по дискриминаторам из IDL.
        Учитываем все сделки с участием SOL (WSOL) на сумму >= 4 SOL.
        """
        if not _SWAP_BY_DISC:
            return

        account_keys = _get_account_keys(raw_tx)
        signer: Optional[str] = account_keys[0] if account_keys else None
        sig = _signature_from_tx(raw_tx)
        slot = _slot_from_tx(raw_tx)
        ts = _timestamp_from_tx(raw_tx)

        # Находим первую swap‑инструкцию Meteora и определяем pool.
        pool_addr: Optional[str] = None
        payer: Optional[str] = None
        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            data = _decode_instruction_data(ix)
            disc = data[:8] if len(data) >= 8 else b""
            mapping = _SWAP_BY_DISC.get(disc)
            if mapping is None:
                continue
            pool_idx = mapping["pool_index"]
            payer_idx = mapping.get("payer_index")
            pool_addr = _resolve_account(account_keys, ix, pool_idx)
            payer = _resolve_account(account_keys, ix, payer_idx) if payer_idx is not None else None
            logger.debug(
                "[METEORA-SWAP-MATCH] tx=%s slot=%s disc=%s pool=%s payer=%s",
                sig,
                slot,
                list(disc),
                pool_addr,
                payer,
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
            # если decimals различаются, берём из post.
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
                "[METEORA-SWAP-CANDIDATE] tx=%s owner=%s wsol_delta=%s lamports (min=%s)",
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

        # Эмитим SwapEvent для каждого подходящего swap‑кейсa.
        if swaps:
            logger.debug(
                "[METEORA-SWAP] tx=%s slot=%s pool=%s swaps=%s",
                sig,
                slot,
                pool_addr,
                swaps,
            )
        for owner, in_mint, in_amt, out_mint, out_amt in swaps:
            # Как и для PumpSwap, в расчётах выше:
            #   in_*  — что трейдер отдал,
            #   out_* — что трейдер получил.
            # В нормализованном SwapEvent (и в БД) ожидаем наоборот:
            #   in_*  — получено,
            #   out_* — отдано.
            yield SwapEvent(
                protocol="meteora",
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

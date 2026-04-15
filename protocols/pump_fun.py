"""
Парсер протокола Pump.Fun (bonding curve).

Программа: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
Инструкции: create (новый токен/пул), buy, sell.
Формат raw_tx: результат getTransaction (message.accountKeys, message.instructions,
опционально meta.innerInstructions). data в инструкциях — base64.
"""

import base64
import hashlib
import struct
from datetime import datetime
from typing import Any, Iterable, List, Optional, Tuple

from core.types import PoolCreatedEvent, SwapEvent
from infra.parser_base import ProtocolParser


# Дискриминаторы Anchor: первые 8 байт sha256("global:<instruction_name>").
def _anchor_discriminator(name: str) -> bytes:
    return hashlib.sha256(f"global:{name}".encode()).digest()[:8]


CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])  # create
BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])     # buy
SELL_DISCRIMINATOR = _anchor_discriminator("sell")

# Wrapped SOL (quote для bonding curve на Solana).
WSOL_MINT = "So11111111111111111111111111111111111111112"


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
        try:
            return base64.b64decode(data)
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


def _u64_le(data: bytes, offset: int) -> Optional[int]:
    if offset + 8 > len(data):
        return None
    return struct.unpack_from("<Q", data, offset)[0]


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


def _has_create_in_logs(raw_tx: dict) -> bool:
    """
    Проверить по meta.logMessages, что в транзакции была инструкция Create или CreateV2
    (именно создание пула/токена), а не CreateTokenAccount и т.п.
    """
    meta = raw_tx.get("meta") or (raw_tx.get("transaction") or {}).get("meta") or {}
    logs = meta.get("logMessages") or []
    for line in logs:
        if not isinstance(line, str):
            continue
        lower = line.lower()
        if "instruction: createv2" in lower:
            return True
        if "instruction: create" in lower:
            # Только "Instruction: Create" или "Instruction: CreateV2", не CreateTokenAccount
            idx = lower.find("instruction: create")
            after = lower[idx + len("instruction: create"):]
            if not after or not after[0].isalpha():
                return True
    return False


class PumpFunParser(ProtocolParser):
    """
    Парсер транзакций Pump.Fun: создание токенов (create) и свапы по кривой (buy/sell).
    """

    @property
    def protocol_name(self) -> str:
        return "pump_fun"

    def __init__(self, program_id: str) -> None:
        self.program_id = program_id

    def _resolve_program_id(self, raw_tx: dict, program_id_ref: Any) -> bool:
        """Проверить, что ссылка на программу указывает на Pump.Fun."""
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
        """
        Транзакция поддерживается, если в ней вообще фигурирует программа Pump.Fun.

        Раньше мы дополнительно проверяли дискриминаторы инструкций (create/buy/sell),
        но на практике сигнатуры методов могут меняться (CreateV2 и т.п.), из‑за чего
        "create" не всегда совпадает с жёстко захардкоженным CREATE_DISCRIMINATOR.

        Для целей бота (новые токены + свапы) достаточно факта присутствия программы.
        """
        account_keys = _get_account_keys(raw_tx)
        if self.program_id in account_keys:
            return True
        # Либо программа встречается в инструкциях по индексу.
        return any(
            self._resolve_program_id(raw_tx, prog_ref)
            for prog_ref, _, _ in _get_all_instructions(raw_tx)
        )

    def parse_pool_creations(self, raw_tx: dict) -> Iterable[PoolCreatedEvent]:
        # Считаем пул созданным только если в logMessages есть Create/CreateV2.
        # Иначе каждая Pump.Fun‑транзакция (в т.ч. Buy/Sell) давала бы ложный [POOL].
        if not _has_create_in_logs(raw_tx):
            return

        account_keys = _get_account_keys(raw_tx)
        sig = _signature_from_tx(raw_tx)
        slot = _slot_from_tx(raw_tx)
        ts = _timestamp_from_tx(raw_tx)

        # В транзакции с CreateV2 первая инструкция Pump.Fun — это создание пула.
        seen_create_for_tx = False

        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            if seen_create_for_tx:
                # Создание пула мы уже зафиксировали для этой транзакции.
                continue

            # Раньше мы полагались на дискриминатор Anchor (CREATE_DISCRIMINATOR),
            # но для CreateV2 он может отличаться. Для надёжности считаем, что
            # первая инструкция Pump.Fun в транзакции с Create/Buy — это создание.
            # Порядок аккаунтов для create:
            # mint(0), bonding_curve(1), associated_bonding_curve(2),
            # global(3), mpl(4), metadata(5), user(6)
            mint = _resolve_account(account_keys, ix, 0)
            bonding_curve = _resolve_account(account_keys, ix, 1)
            user = _resolve_account(account_keys, ix, 6)
            if not mint or not bonding_curve:
                continue

            seen_create_for_tx = True

            yield PoolCreatedEvent(
                protocol="pump_fun",
                pool_address=bonding_curve,
                base_mint=mint,
                quote_mint=WSOL_MINT,
                tx_signature=sig,
                slot=slot,
                timestamp=ts,
                extra={"creator": user},
            )

    def parse_swaps(self, raw_tx: dict) -> Iterable[SwapEvent]:
        account_keys = _get_account_keys(raw_tx)
        sig = _signature_from_tx(raw_tx)
        slot = _slot_from_tx(raw_tx)
        ts = _timestamp_from_tx(raw_tx)

        for prog_ref, ix, _ in _get_all_instructions(raw_tx):
            if not self._resolve_program_id(raw_tx, prog_ref):
                continue
            data = _decode_instruction_data(ix)
            if len(data) < 24:
                continue

            disc = data[:8]
            # buy(user, associated_user, mint, ...): amount = token amount, max_sol_cost = lamports
            # Порядок аккаунтов: user(0), associated_user(1), mint(2), bonding_curve(3), ...
            if disc == BUY_DISCRIMINATOR:
                amount_tokens = _u64_le(data, 8)
                amount_sol = _u64_le(data, 16)
                if amount_tokens is None or amount_sol is None:
                    continue
                mint = _resolve_account(account_keys, ix, 2)
                bonding_curve = _resolve_account(account_keys, ix, 3)
                user = _resolve_account(account_keys, ix, 0)
                if not mint or not bonding_curve:
                    continue
                yield SwapEvent(
                    protocol="pump_fun",
                    pool_address=bonding_curve,
                    in_mint=WSOL_MINT,
                    out_mint=mint,
                    in_amount=amount_sol,
                    out_amount=amount_tokens,
                    
                    trader=user or None,
                    tx_signature=sig,
                    slot=slot,
                    timestamp=ts,
                )
            elif disc == SELL_DISCRIMINATOR:
                amount_tokens = _u64_le(data, 8)
                min_sol = _u64_le(data, 16)
                if amount_tokens is None or min_sol is None:
                    continue
                mint = _resolve_account(account_keys, ix, 2)
                bonding_curve = _resolve_account(account_keys, ix, 3)
                user = _resolve_account(account_keys, ix, 0)
                if not mint or not bonding_curve:
                    continue
                yield SwapEvent(
                    protocol="pump_fun",
                    pool_address=bonding_curve,
                    in_mint=mint,
                    out_mint=WSOL_MINT,
                    in_amount=amount_tokens,
                    out_amount=min_sol,
                    
                    trader=user or None,
                    tx_signature=sig,
                    slot=slot,
                    timestamp=ts,
                )

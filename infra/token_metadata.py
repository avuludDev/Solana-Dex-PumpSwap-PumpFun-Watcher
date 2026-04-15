"""
Подтягивание названия и символа токена из Metaplex Token Metadata (RPC getAccountInfo).
PDA: ["metadata", METADATA_PROGRAM_ID, mint].
Структура аккаунта: key(1) + update_authority(32) + mint(32) + name_len(4) + name + symbol_len(4) + symbol + ...
"""

import base64
import struct
from typing import Optional, Tuple

import aiohttp

METADATA_PROGRAM_ID = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"


def _decode_metadata_name_symbol(data: bytes) -> Tuple[Optional[str], Optional[str]]:
    """
    Распарсить из сырых данных Metaplex Metadata поля name и symbol.
    Layout: key(1) + update_authority(32) + mint(32) = 65, затем name_len(4) + name, symbol_len(4) + symbol.
    """
    if len(data) < 65 + 8:  # минимум 65 + 4 (name_len) + 4 (symbol_len)
        return None, None
    try:
        off = 65
        (name_len,) = struct.unpack_from("<I", data, off)
        off += 4
        if off + name_len > len(data):
            return None, None
        name = data[off : off + name_len].decode("utf-8", errors="replace").strip()
        off += name_len
        if off + 4 > len(data):
            return name, None
        (symbol_len,) = struct.unpack_from("<I", data, off)
        off += 4
        if off + symbol_len > len(data):
            return name, None
        symbol = data[off : off + symbol_len].decode("utf-8", errors="replace").strip()
        return name, symbol
    except Exception:
        return None, None


async def fetch_metadata_name_symbol(
    http_endpoint: str,
    session: aiohttp.ClientSession,
    mint: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Получить (name, symbol) токена по mint через RPC getProgramAccounts
    по программе Metaplex Metadata с фильтром memcmp по mint (offset 33).
    """
    if not mint or not http_endpoint:
        return None, None
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            METADATA_PROGRAM_ID,
            {
                "encoding": "base64",
                "filters": [
                    {"memcmp": {"offset": 33, "bytes": mint}},
                ],
                "dataSlice": {"offset": 0, "length": 500},
            },
        ],
    }
    try:
        async with session.post(http_endpoint, json=payload) as resp:
            if resp.status != 200:
                return None, None
            data = await resp.json()
    except Exception:
        return None, None
    result = data.get("result")
    if not result or not isinstance(result, list) or len(result) == 0:
        return None, None
    account = result[0]
    acc_data = account.get("account", {}).get("data")
    if isinstance(acc_data, list):
        raw = bytes(acc_data)
    elif isinstance(acc_data, str):
        try:
            raw = base64.b64decode(acc_data)
        except Exception:
            return None, None
    else:
        return None, None
    return _decode_metadata_name_symbol(raw)

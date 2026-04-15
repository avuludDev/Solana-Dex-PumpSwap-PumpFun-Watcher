"""
Получение name/symbol/decimals/supply токена через Helius DAS (getAsset) по mint.

Мы используем тот же HTTP‑endpoint, что и для RPC:
https://mainnet.helius-rpc.com/?api-key=...
"""

from typing import Optional, Tuple

import aiohttp


async def fetch_token_name_symbol(
    http_endpoint: str,
    session: aiohttp.ClientSession,
    mint: str,
) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[float]]:
    """
    Вернуть (name, symbol, decimals, total_supply) для fungible‑токена по mint через Helius DAS `getAsset`.

    Структура ответа может немного отличаться в зависимости от настроек,
    поэтому пробуем несколько типичных путей:
    - result.content.metadata.name / symbol
    - result.token_info.name / symbol / decimals / supply
    """
    if not http_endpoint or not mint:
        return None, None, None, None

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": {
            "id": mint,
        },
    }

    try:
        async with session.post(http_endpoint, json=payload) as resp:
            if resp.status != 200:
                # Не спамим логами здесь — бот может работать без метаданных.
                return None, None, None, None
            data = await resp.json()
    except Exception:
        return None, None, None, None

    asset = data.get("result") or {}

    # Вариант 1: content.metadata.name/symbol
    content = asset.get("content") or {}
    metadata = content.get("metadata") or {}
    name = metadata.get("name")
    symbol = metadata.get("symbol")

    decimals: Optional[int] = None
    total_supply: Optional[float] = None

    # Вариант 2: token_info.name/symbol/decimals/supply
    token_info = asset.get("token_info") or asset.get("tokenInfo") or {}
    if name is None:
        name = token_info.get("name")
    if symbol is None:
        symbol = token_info.get("symbol")
    if "decimals" in token_info:
        decimals = token_info.get("decimals")

    # Supply в DAS может быть в разных полях, пробуем типичные варианты.
    supply_raw = (
        token_info.get("supply")
        or token_info.get("circulating_supply")
        or token_info.get("circulatingSupply")
    )

    # Приводим к строке/инту/флоату, если вдруг там что‑то другое.
    if name is not None:
        name = str(name)
    if symbol is not None:
        symbol = str(symbol)
    if decimals is not None:
        try:
            decimals = int(decimals)
        except (TypeError, ValueError):
            decimals = None

    if supply_raw is not None:
        try:
            # Храним supply в "человеческом" виде (с учётом decimals),
            # если он уже приведён, или как число единиц, если нет.
            total_supply = float(supply_raw)
        except (TypeError, ValueError):
            total_supply = None

    return name, symbol, decimals, total_supply


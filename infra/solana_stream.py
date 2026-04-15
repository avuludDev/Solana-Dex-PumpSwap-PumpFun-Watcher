import asyncio
import json
from typing import Any, Awaitable, Callable, List, Optional

import aiohttp
import websockets
from websockets.client import WebSocketClientProtocol


class SolanaStream:
    """
    Поток транзакций Solana через Helius Enhanced WebSocket (`transactionSubscribe`).

    Используется фильтр по `accountInclude` — сюда можно передать programId'ы
    (Pump.Fun, PumpSwap, Meteora и т.д.), чтобы получать только нужные транзакции.
    """

    def __init__(
        self,
        ws_endpoint: str,
        account_include: List[str],
        commitment: str = "confirmed",
    ) -> None:
        self._ws_endpoint = ws_endpoint
        # Helius logsSubscribe поддерживает только ОДИН pubkey в mentions.
        # Поэтому берём первый из списка (для нескольких программ можно
        # создавать несколько экземпляров SolanaStream).
        self._account_include = account_include[0:1]
        self._commitment = commitment
        # HTTP RPC endpoint Helius для getTransaction.
        # Его имеет смысл пробрасывать отдельно, но пока используем тот же URL,
        # только с https вместо wss, если явно не задан.
        self._http_endpoint: Optional[str] = None

    def set_http_endpoint(self, http_endpoint: str) -> "SolanaStream":
        self._http_endpoint = http_endpoint
        return self

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        """
        Отправить запрос logsSubscribe на Helius с фильтром по programId (mentions).
        """
        request: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {
                    "mentions": self._account_include,
                },
                {
                    "commitment": self._commitment,
                },
            ],
        }
        await ws.send(json.dumps(request))

    async def _fetch_transaction(
        self,
        session: aiohttp.ClientSession,
        signature: str,
    ) -> Optional[dict[str, Any]]:
        """
        Получить полную транзакцию по сигнатуре через HTTP RPC (getTransaction).
        """
        if not signature:
            return None

        endpoint = self._http_endpoint
        if not endpoint:
            # Если явный HTTP endpoint не задан, пробуем заменить wss на https.
            endpoint = self._ws_endpoint.replace("wss://", "https://")

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    # ВАЖНО: по умолчанию getTransaction использует commitment "finalized".
                    # Но logsSubscribe, как правило, возвращает сигнатуры для уровней
                    # "processed"/"confirmed". Из‑за этого сразу после логов
                    # getTransaction может возвращать result = None.
                    # Явно выставляем commitment такой же, как у WebSocket‑подписки,
                    # чтобы получать свежие транзакции.
                    "commitment": self._commitment,
                },
            ],
        }

        try:
            async with session.post(endpoint, json=payload) as resp:
                if resp.status != 200:
                    # Временный лог, чтобы увидеть неуспешные ответы RPC.
                    try:
                        text = await resp.text()
                    except Exception:
                        text = "<cannot read body>"
                    print(f"[HTTP] getTransaction FAILED status={resp.status} body={text[:500]}")
                    return None
                data = await resp.json()
        except Exception as e:
            # Временный лог исключений, чтобы понять, почему getTransaction падает.
            print(f"[HTTP] getTransaction exception for {signature}: {e}")
            return None

        result = data.get("result")
        if not isinstance(result, dict):
            # Логируем случаи, когда в ответе нет result (например, ошибка или не найдено).
            print(f"[HTTP] getTransaction no result for signature={signature}. Raw={str(data)[:500]}")
            return None
        return result

    async def run(
        self,
        on_raw_tx: Callable[[dict, aiohttp.ClientSession], Awaitable[None]],
    ) -> None:
        """
        Подключается к Helius WebSocket, подписывается на transactionSubscribe
        и на каждое уведомление вызывает on_raw_tx(result).

        В result структура соответствует примерам Helius:
        {
          "transaction": { "transaction": {...}, "meta": {...} },
          "signature": "...",
          "slot": 123,
          "transactionIndex": 0,
          ...
        }
        """
        while True:
            try:
                print(f"[WS] Подключение к {self._ws_endpoint} ...")
                # ping_interval гарантирует регулярные ping'и (health-check) для Helius.
                async with websockets.connect(self._ws_endpoint, ping_interval=60) as ws, aiohttp.ClientSession() as session:
                    print("[WS] Соединение установлено, отправляем подписку logsSubscribe.")
                    await self._subscribe(ws)

                    async for message in ws:
                        try:
                            payload = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        params = payload.get("params") or {}
                        result = params.get("result") or {}
                        value = result.get("value") or {}
                        signature = value.get("signature")

                        tx = await self._fetch_transaction(session, signature)
                        if tx is None:
                            continue

                        # Передаём результат getTransaction и сессию для доп. RPC (метаданные и т.д.).
                        await on_raw_tx(tx, session)

            except Exception as exc:
                # Логируем сбой соединения и пытаемся переподключиться через паузу.
                print(f"[WS] Ошибка/разрыв соединения: {exc}. Повторное подключение через 3 секунды.")
                await asyncio.sleep(3)
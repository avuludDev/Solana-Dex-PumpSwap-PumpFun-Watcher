"""
Вспомогательный скрипт для очистки всех вебхуков в Helius‑проекте.

Использование:

    python -m webhook_parser.cleanup_webhooks

Скрипт:
1) Берёт RPC endpoint из config.settings.load_settings().solana.http_endpoint
   и вытаскивает из него api-key.
2) Делает GET /v0/webhooks, выводит список.
3) Спрашивает подтверждение в консоли.
4) При подтверждении удаляет КАЖДЫЙ webhook через DELETE /v0/webhooks/{id}.

ВНИМАНИЕ: Операция необратима для текущего Helius‑проекта.
"""

import asyncio
import urllib.parse
from typing import List

import aiohttp

from config.settings import load_settings


async def _get_api_key() -> str | None:
    settings = load_settings()
    parsed = urllib.parse.urlparse(settings.solana.http_endpoint)
    query = urllib.parse.parse_qs(parsed.query)
    api_keys = query.get("api-key") or query.get("api_key") or []
    return api_keys[0] if api_keys else None


async def _cleanup_all_webhooks() -> None:
    api_key = await _get_api_key()
    if not api_key:
        print(
            "[WEBHOOK-CLEAN] Не удалось извлечь api-key из settings.solana.http_endpoint. "
            "Проверь конфиг."
        )
        return

    base_url = f"https://api-mainnet.helius-rpc.com/v0/webhooks"

    async with aiohttp.ClientSession() as session:
        print(f"[WEBHOOK-CLEAN] GET {base_url}?api-key=***")
        async with session.get(f"{base_url}?api-key={api_key}", timeout=30) as resp:
            text = await resp.text()
            if resp.status != 200:
                print(f"[WEBHOOK-CLEAN] Не удалось получить список вебхуков: {resp.status} {text}")
                return

            try:
                existing: List[dict] = await resp.json()
            except Exception:
                import json

                existing = json.loads(text)

        if not existing:
            print("[WEBHOOK-CLEAN] Вебхуков не найдено.")
            return

        print(f"[WEBHOOK-CLEAN] Найдено {len(existing)} вебхуков:")
        for wh in existing:
            print(
                f"  - id={wh.get('webhookID')} url={wh.get('webhookURL')} "
                f"type={wh.get('webhookType')} addrs={wh.get('accountAddresses')}"
            )

        answer = input(
            "\n[WEBHOOK-CLEAN] Удалить ВСЕ перечисленные вебхуки? (yes/[no]): "
        ).strip().lower()
        if answer not in ("y", "yes"):
            print("[WEBHOOK-CLEAN] Отменено пользователем.")
            return

        for wh in existing:
            wid = wh.get("webhookID")
            if not wid:
                continue

            url = f"{base_url}/{wid}?api-key={api_key}"
            print(f"[WEBHOOK-CLEAN] DELETE {url}")
            async with session.delete(url, timeout=30) as resp:
                text = await resp.text()
                if resp.status not in (200, 204):
                    print(
                        f"[WEBHOOK-CLEAN] Ошибка при удалении webhookID={wid}: "
                        f"{resp.status} {text}"
                    )
                else:
                    print(f"[WEBHOOK-CLEAN] Удалён webhookID={wid}")


def main() -> None:
    asyncio.run(_cleanup_all_webhooks())


if __name__ == "__main__":
    main()


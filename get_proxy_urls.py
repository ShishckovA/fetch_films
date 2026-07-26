#!/usr/bin/env python3

import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


PROXY_SOURCE_URL = "https://proxymania.su/en/free-proxy?page={page}"
OUTPUT_FILE = Path(__file__).with_name("proxies.txt")
WORKERS = 32
PAGE_TIMEOUT = 30
PROXY_TIMEOUT = 10
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
)


def normalize_proxy(proxy: str) -> str:
    proxy = proxy.strip()
    return proxy if "://" in proxy else f"http://{proxy}"


def fetch_candidates() -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    with requests.Session() as session:
        session.trust_env = False
        session.headers["User-Agent"] = USER_AGENT
        for page in count(1):
            try:
                response = session.get(
                    PROXY_SOURCE_URL.format(page=page),
                    timeout=PAGE_TIMEOUT,
                )
                response.raise_for_status()
            except requests.RequestException:
                if candidates:
                    break
                raise
            found = [
                normalize_proxy(cell.get_text(strip=True))
                for cell in BeautifulSoup(
                    response.text,
                    "html.parser",
                ).select("td.proxy-cell")
                if cell.get_text(strip=True)
            ]
            new = [proxy for proxy in found if proxy not in seen]
            if not new:
                break
            seen.update(new)
            candidates.extend(new)
    return candidates


def check_proxy(proxy: str, check_url: str) -> str | None:
    try:
        with requests.Session() as session:
            session.trust_env = False
            session.headers["User-Agent"] = USER_AGENT
            with session.get(
                check_url,
                proxies={"http": proxy, "https": proxy},
                timeout=PROXY_TIMEOUT,
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                if not response.content.strip():
                    return None
        return proxy
    except requests.RequestException:
        return None


def refresh(check_url: str) -> None:
    candidates = fetch_candidates()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        working = [
            proxy
            for proxy in pool.map(
                lambda proxy: check_proxy(proxy, check_url),
                candidates,
            )
            if proxy
        ]

    if not working:
        print(f"Проверено {len(candidates)}; рабочих нет, старый список сохранён")
        return

    temporary = OUTPUT_FILE.with_name(f"{OUTPUT_FILE.name}.tmp")
    temporary.write_text(
        "".join(f"{proxy}\n" for proxy in working),
        encoding="utf-8",
    )
    temporary.replace(OUTPUT_FILE)
    print(f"Проверено {len(candidates)}; сохранено {len(working)} в {OUTPUT_FILE}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Обновлять список рабочих прокси")
    parser.add_argument("check_url")
    parser.add_argument("refresh_seconds", type=float)
    args = parser.parse_args()

    parsed = urlparse(args.check_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        parser.error("check_url должен быть абсолютным HTTP(S) URL")
    if args.refresh_seconds <= 0:
        parser.error("refresh_seconds должен быть больше нуля")

    while True:
        try:
            refresh(args.check_url)
        except requests.RequestException as error:
            print(f"Не удалось обновить прокси: {error}")
        time.sleep(args.refresh_seconds)


if __name__ == "__main__":
    main()

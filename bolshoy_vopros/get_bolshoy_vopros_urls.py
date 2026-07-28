#!/usr/bin/env python3

import argparse
import random
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bolshoyvopros.ru"
PROXIES_FILE = Path(__file__).resolve().parent.parent / "proxies.txt"
MAX_PAGES = 10_000
RETRIES = 100
TIMEOUT = 3
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
    )
}


def read_proxies() -> list[str]:
    if not PROXIES_FILE.exists():
        return []
    proxies = []
    for line in PROXIES_FILE.read_text(encoding="utf-8").splitlines():
        proxy = line.strip()
        if proxy and not proxy.startswith("#"):
            proxies.append(proxy if "://" in proxy else f"http://{proxy}")
    return proxies


def fetch_page(session: requests.Session, tag: str, page: int) -> list[str]:
    url = f"{BASE_URL}/questions/actual/tag{tag}_p{page}.html"
    for attempt in range(RETRIES):
        try:
            proxies = read_proxies()
            proxy = random.choice(proxies) if proxies else None
            response = session.get(
                url,
                proxies={"http": proxy, "https": proxy} if proxy else None,
                timeout=TIMEOUT,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            return [
                urljoin(BASE_URL, element["href"])
                for element in soup.select("a.question_title[href]")
            ]
        except requests.RequestException:
            pass
    raise RuntimeError(f"Не удалось скачать страницу {page}")


def save_urls(path: Path, urls: list[str]) -> None:
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text("".join(f"{url}\n" for url in urls), encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Собрать все URL Bolshoy Vopros по тегу"
    )
    parser.add_argument("tag")
    args = parser.parse_args()
    if not args.tag.isdigit():
        parser.error("tag должен быть числом")

    output = Path(__file__).with_name(f"bolshoy_vopros_tag_{args.tag}_urls.txt")
    urls = output.read_text(encoding="utf-8").splitlines() if output.exists() else []
    seen = set(urls)
    previous_page: list[str] | None = None

    session = requests.Session()
    session.trust_env = False
    session.headers.update(HEADERS)
    try:
        for page in range(1, MAX_PAGES + 1):
            page_urls = fetch_page(session, args.tag, page)
            if not page_urls or page_urls == previous_page:
                break
            previous_page = page_urls
            new_urls = [url for url in page_urls if url not in seen]
            if new_urls:
                seen.update(new_urls)
                urls.extend(new_urls)
                save_urls(output, urls)
            print(f"\rСтраница {page}; URL: {len(urls)}", end="", flush=True)
    finally:
        session.close()

    save_urls(output, urls)
    print(f"\nСохранено в {output}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import argparse
import hashlib
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

WORKERS = 32
RETRIES = 128
TIMEOUT = (10, 30)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0 Safari/537.36"
    )
}

thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        session.trust_env = False
        session.headers.update(HEADERS)
        thread_local.session = session
    return thread_local.session


def read_urls(path: Path) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(),
        start=1,
    ):
        url = line.strip()
        if not url or url.startswith("#") or url in seen:
            continue
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"{path}:{line_number}: нужен абсолютный HTTP(S) URL")
        seen.add(url)
        urls.append(url)
    return urls


def read_proxies(path: Path) -> list[str]:
    proxies = []
    for line in path.read_text(encoding="utf-8").splitlines():
        proxy = line.strip()
        if not proxy or proxy.startswith("#"):
            continue
        if "://" not in proxy:
            proxy = f"http://{proxy}"
        proxies.append(proxy)
    if not proxies:
        raise ValueError(f"В {path} нет прокси")
    return proxies


def filename_for(url: str) -> str:
    return f"{hashlib.sha256(url.encode()).hexdigest()}.html"


def atomic_write(path: Path, content: bytes) -> None:
    temporary = path.with_name(f"{path.name}.part")
    temporary.write_bytes(content)
    temporary.replace(path)


def download_one(
    url: str,
    proxies_file: Path,
    output_dir: Path,
) -> tuple[str, str]:
    output_path = output_dir / filename_for(url)
    if output_path.exists() and output_path.stat().st_size:
        return "skipped", url

    for attempt in range(RETRIES):
        try:
            proxy = random.choice(read_proxies(proxies_file))
            with get_session().get(
                url,
                proxies={"http": proxy, "https": proxy},
                timeout=TIMEOUT,
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                content = response.content
            if not content.strip():
                raise RuntimeError("empty response")
            atomic_write(output_path, content)
            return "downloaded", url
        except (OSError, RuntimeError, ValueError, requests.RequestException):
            if attempt + 1 < RETRIES:
                time.sleep(min(2**attempt + random.random(), 10))

    return "failed", url


def main() -> None:
    parser = argparse.ArgumentParser(description="Скачать HTML по списку URL")
    parser.add_argument("urls_file", type=Path, help="список URL для скачивания")
    parser.add_argument("proxies_file", type=Path, help="список прокси")
    parser.add_argument("output_dir", type=Path, help="папка для HTML")
    parser.add_argument(
        "ignore_urls_file",
        type=Path,
        nargs="?",
        help="необязательный список уже скачанных URL",
    )
    args = parser.parse_args()

    input_urls = read_urls(args.urls_file)
    ignored_urls = (
        set(read_urls(args.ignore_urls_file))
        if args.ignore_urls_file
        else set()
    )
    urls = [url for url in input_urls if url not in ignored_urls]
    ignored_count = len(input_urls) - len(urls)
    proxies = read_proxies(args.proxies_file)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    manifest = "filename\turl\n" + "".join(
        f"{filename_for(url)}\t{url}\n" for url in urls
    )
    atomic_write(args.output_dir / "manifest.tsv", manifest.encode())

    counts = {"downloaded": 0, "skipped": 0, "failed": 0}
    failed_urls = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [
            pool.submit(download_one, url, args.proxies_file, args.output_dir)
            for url in urls
        ]
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="HTML",
            unit="page",
        ):
            status, url = future.result()
            counts[status] += 1
            if status == "failed":
                failed_urls.append(url)

    failed_path = args.output_dir / "failed_urls.txt"
    if failed_urls:
        atomic_write(failed_path, ("\n".join(failed_urls) + "\n").encode())
    elif failed_path.exists():
        failed_path.unlink()

    print(
        f"Скачано: {counts['downloaded']}; "
        f"пропущено: {counts['skipped']}; "
        f"исключено: {ignored_count}; "
        f"ошибок: {counts['failed']}; "
        f"прокси при старте: {len(proxies)}"
    )


if __name__ == "__main__":
    main()

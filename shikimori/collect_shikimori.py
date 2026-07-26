import requests
from bs4 import BeautifulSoup
import tqdm
import time
from pathlib import Path


PAGES_DIR = Path(__file__).resolve().parent / "pages"
files = {path.name for path in PAGES_DIR.iterdir()}

data = {}

for i in range(1, 70000):
    with (PAGES_DIR / f"{i}.html").open() as fin:
        text = fin.read()
        if "Информация" in text:
            assert str(i) in text
            data[i] = "200"
            continue
        if "404.jpg" in text:
            data[i] = "404"
            continue
        if "ограничен 18+" in text:
            print(f"Restricted {i}")
            continue
        
        soup = BeautifulSoup(text, "lxml")
        urls = soup.find_all("a")
        assert len(urls) == 1, i
        url = urls[0]["href"]
        filename = url.split("/")[-1].split("-")[0]


        if f"{filename}.html" not in files:
            print(f"No file: {i}")
            continue
        with (PAGES_DIR / f"{filename}.html").open() as fin2:
            text2 = fin2.read()
            if "Информация" not in text2 or str(i) not in text2:
                print(f"Bad id: {i}")

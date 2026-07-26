import requests
from bs4 import BeautifulSoup
import tqdm
import time
from pathlib import Path


PAGES_DIR = Path(__file__).resolve().parent / "pages"
PAGES_DIR.mkdir(parents=True, exist_ok=True)

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'max-age=0',
    'if-none-match': 'W/"099bbac092c2167cec4cb392050790bd"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="148", "YaBrowser";v="26.6", "Not/A)Brand";v="99", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Mobile Safari/537.36',
    # 'cookie': '__ddg9_=46.22.62.85; __ddg1_=9v0c9atSppGUnkWE6RUP; _ym_uid=1783665280767774538; _ym_d=1783665280; _ym_isad=1; _kawai_session=ph6s%2BbcHcdl2ifCxC50SgjLw6n7T%2FiV179dCCFiDLjLTM%2FFwi5RFqElV6mg7UCHvbS2AbXqhrKnPrWgHGrZ2dBudNPW8ASgkF59R76q%2F3xzEJ2kqmIJuen4L0MRj2Agy0CPCmhbjs3HqL7hgp5vPREEyu8ZpmkUr3pvAFaVodSIGjledHXEB8AbYSIp3TNdaX%2BXlKPpTiAvqWIju7CdVQu%2B%2BPj83ki0lWYNJaPuPDVa0nHSEgwea%2FEc%2BewpaL%2BeDdQ7%2FZ9HqcvFqZ69oVdZsMCZZU3OxXXwGyDBIPQNQgRYeP2SktZnXxsqJEnrOCG7foDv%2B%2BQSDSmyC3YSiao3lyOBpppj4%2FXFi1wUmmp8Seq9YlHQeDikJ3GFEAVu2UClmmzrrDXKBAoD6GJItfBSLusmC72Z9bD4%2BaPHtCfzWEkpC8ntuU2sBtfwlBUnwVTAGTZLC3qY%3D--hH5RWmB0Iw8ocGjE--91E9JuUvcmEE55DDPEkcDQ%3D%3D; __ddg10_=1783665611; __ddg8_=xa2QhdAH970TKENM',
}
sleep = 10

files = {path.name for path in PAGES_DIR.iterdir()}

for i in tqdm.trange(1, 70000):
    with (PAGES_DIR / f"{i}.html").open() as fin:
        text = fin.read()
        if "<h1>Страница переехала</h1>" in text:
            assert "новой ссылке</a>" in text
            soup = BeautifulSoup(text, "lxml")
            urls = soup.find_all("a")
            assert len(urls) == 1
            url = urls[0]["href"]
            filename = url.split("/")[-1].split("-")[0]
            if f"{filename}.html" in files:
                continue
            while 1:
                time.sleep(0.333)
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code != 200:
                        print("Warning: ", response.status_code, url)
                        break
                    soup = BeautifulSoup(response.text, "lxml")
                    assert soup.text.strip() != "Retry later"

                    with (PAGES_DIR / f"{filename}.html").open("w") as fout:
                        fout.write(response.text)
                        break
                except AssertionError:
                    time.sleep(sleep)
                    sleep *= 1.5
                    print("Retry")
            sleep = 10

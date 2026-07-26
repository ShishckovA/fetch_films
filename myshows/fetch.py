import requests
from tqdm import tqdm
import random
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "news"
DATA_DIR.mkdir(parents=True, exist_ok=True)

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "YaBrowser";v="26.4", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36',
}


todo = list(range(1, 12559))
got = [int(path.stem) for path in DATA_DIR.glob("*.html") if path.stem.isdigit()]
todo = list(set(todo) - set(got))
print(got)
random.shuffle(todo)

for i in tqdm(todo):

    response = requests.get(
        f'https://myshows.me/news/{i}/',
        headers=headers,
    )
    with (DATA_DIR / f"{i}.html").open("w") as fout:
        fout.write(response.text)

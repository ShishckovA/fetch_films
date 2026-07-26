import yt.wrapper
import requests
import tqdm
import time
import os
import json

BASE_SLEEP = 10
SLEEP = 10


def get_page(url):
	global SLEEP
	while 1:
		r = requests.get(url, headers=headers)
		if r.status_code != 429:
			break
		time.sleep(SLEEP)
		SLEEP *= 1.2
	SLEEP = BASE_SLEEP
	time.sleep(0.333)
	return r.text


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'if-none-match': 'W/"aa2b242a8edfb90baf80761b56f71354"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="148", "YaBrowser";v="26.6", "Not/A)Brand";v="99", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 YaBrowser/26.6.0.0 Safari/537.36',
    # 'cookie': '__ddg1_=9v0c9atSppGUnkWE6RUP; _ym_uid=1783665280767774538; _ym_d=1783665280; __ddg9_=46.22.62.85; _ym_isad=1; _kawai_session=5f1X0vzzBHysS7q3FP%2BV8kvaaaRfykcvpsUm1M0tFNoQeBqRtlh8Z%2F2UWbkIN9IvMf757lBHt23RRHrIa1GlJDWwtMgQaW8%2BPvlxJkn0Or4PWb9AdsS8zSwirN5rHuniEaVxVnFmKpX3fVJx6loM%2F4B1453COA6wJt2U7H2cf6hJ%2Fo9E8NsjQOqhpsH%2BWPcKw6%2Bsx0Tt3UwNdQ7tmseO7eU4n9irIxpY%2F%2FxSI5GQcqTuoFvWE%2Bhr6jp0l8gM3MfBw%2FcWpzqz%2FrTwpYiwuT5j11anGLFDzdNZog8GSGJJDJYPW5%2BPuPHOMnFesCJZP4nZWLHWkYUePNKhOq6pe%2FAcSiaMrECDER%2Bv7UNWEllire46%2FuJH364ZF7wL0mfbIvUlRkV28snfzu7A5hNeUo68MG0%3D--lSWUBbzaGawXNsB3--QMKIKpNzZ4Hl%2BIKPTfHQhg%3D%3D; __ddg8_=KiaUjYKGOT7oiYKe; __ddg10_=1783927686',
}

sleep = 20
for elem in tqdm.tqdm(yt.wrapper.read_table("//home/hc/ml-research/tmp-alexey.shishkov/shikimori_dump/animes"), total=30000):
	slug = elem["url"].split("/")[-1]
	i = 1
	cur_anime_path = f"reviews/{slug}"
	try:
		os.mkdir(cur_anime_path)
	except:
		import traceback
		traceback.print_exc()
		continue
	while 1:
		data = get_page(f'https://shikimori.io/animes/{slug}/reviews/page/{i}.json')
		with open(f"{cur_anime_path}/{i}.json", "w") as fout:
			fout.write(data)
		if "Сообщить об ошибке" in data or "age_restricted" in data:
			break
		i += 1
		try:
			data = json.loads(data)
		except:
			print(slug, i)
			print(data)			
			raise
		if "postloader" not in data:
			break
	

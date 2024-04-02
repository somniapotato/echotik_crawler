import requests
from utils.parse import parser, parseCats
# from utils.database import write_rows
from dataclasses import dataclass
import datetime
from typing import List
import asyncio
import aiohttp
from aiohttp import ClientTimeout
import random
import logging
from utils.logger import setup_logging
from utils.database import DatabaseManager

filter_url = "https://echotik.live/api/v1/data/videos/leaderboard/filters"
page_url = "https://echotik.live/api/v1/data/videos/leaderboard/sell-videos"
PROXY_TXT = 'proxy-list.txt'
HOST = "localhost"
USER = "root"
PASSWORD = "123456"
DATABASE = "echotik_crawler"


@dataclass
class PageTaskPara:
    time_range: str
    page: str
    product_categories: str
    per_page: str = "10"
    time_type: str = "daily"


headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7",
    "authorization": "Bearer 108205|AfdsWVtJs2wWALqeimj4JNg3E3nGmMElPO7eG6Mq",
    "content-type": "application/json",
    "cookie": "currency=USD; lang=zh-CN; region=US;",
    "dnt": "1",
    "referer": "https://echotik.live/videos/leaderboard/top-selling-videos?time_type=daily&time_range=20240326&page=1",
    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "x-currency": "USD",
    "x-lang": "zh-CN",
    "x-region": "US",
}


def getProxies() -> List[str]:
    file_path = PROXY_TXT
    with open(file_path, 'r', encoding='utf-8') as f:
        proxies = f.readlines()
    proxies = [p.rstrip('\n') for p in proxies]
    return proxies


async def fetch(url, page_params, session, proxy):
    try:
        async with session.get(url, params=page_params, timeout=ClientTimeout(total=10), headers=headers) as response:
            # async with session.get(url, params=page_params, proxy=proxy, timeout=ClientTimeout(total=10), headers=headers) as response:
            logging.info(
                f"request success: cat_id={page_params.product_categories}, page={page_params.page}, per_page={page_params.per_page}")
            return await response.text()
    except Exception as e:
        logging.warning(f"request failed:{proxy} - {e}")


async def page_task(para: PageTaskPara, proxies: List[str], session: aiohttp.ClientSession, dataBase: DatabaseManager):
    page_params = {
        "time_type": para.time_type,
        "time_range": para.time_range,
        "page": para.page,
        "product_categories": para.product_categories,
        "per_page": para.per_page
    }

    proxy = random.choice(proxies)
    url = page_url
    resp = await fetch(url, page_params, session, f"http://{proxy}")
    records = parser(resp)
    print(records[0])
    print(len(records))
    for record in records:
        dataBase.insert_or_update_row(record)


async def main():
    dataBase = DatabaseManager(HOST, USER, PASSWORD, DATABASE)

    current_date = datetime.date.today()
    ti_range = str(current_date - datetime.timedelta(days=1)).replace("-", "")

    proxies = getProxies()

    response = requests.get(filter_url, headers=headers)
    cats = parseCats(response.text)
    logging.info(f'{len(cats)} cats has been spawned')
    pageTasks = []

    session = aiohttp.ClientSession()

    # for i in range(len(cats)):
    for i in range(2):
        para = PageTaskPara(time_range=ti_range, page="1",
                            product_categories=cats[i])
        task = asyncio.create_task(page_task(para, proxies, session, dataBase))
        pageTasks.append(task)
    try:
        await asyncio.gather(*pageTasks, return_exceptions=True)
    finally:
        await session.close()

    dataBase.close()

if __name__ == '__main__':
    setup_logging()
    asyncio.run(main())

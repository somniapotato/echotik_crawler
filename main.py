import requests
from utils.parse import parser, parseCats
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
from tenacity import retry, stop_after_attempt, wait_fixed
import toml

filter_url = "https://echotik.live/api/v1/data/videos/leaderboard/filters"
page_url = "https://echotik.live/api/v1/data/videos/leaderboard/sell-videos"

with open('config.toml', 'r') as f:
    data = toml.load(f)

db_host = data['database']['host']
db_user = data['database']['username']
db_password = data['database']['password']
db_name = data['database']['dbname']

proxy_txt = data['other']['proxy_txt']
debug_mode = data['other']['debug_mode']

per_page = data['task']['per_page']
authorization = data['task']['authorization']


@dataclass
class PageTaskPara:
    time_range: str
    page: str
    product_categories: str
    per_page: str = f"{per_page}"
    time_type: str = "daily"


headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7",
    "authorization": authorization,
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
    file_path = proxy_txt
    with open(file_path, 'r', encoding='utf-8') as f:
        proxies = f.readlines()
    proxies = [p.rstrip('\n') for p in proxies]
    if len(proxies) == 0:
        raise Exception("proxy list not found")
    return proxies


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch(url, page_params, session, proxy):
    if debug_mode:
        proxy = ""
    try:
        async with session.get(url, params=page_params, proxy=proxy, timeout=ClientTimeout(total=10), headers=headers) as response:
            logging.info(
                f"request success: cat_id={page_params['product_categories']}, page={page_params['page']}, per_page={page_params['per_page']}")

            return await response.text()
    except Exception as e:
        logging.error(f"request failed:{proxy} - {e}")


async def page_task(para: PageTaskPara, proxies: List[str], session: aiohttp.ClientSession):
    try:
        dataBase = DatabaseManager(db_host, db_user, db_password, db_name)
    except Exception as e:
        logging.error(f"connect to mysql failed: {e}")

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
    try:
        records = parser(resp)
    except Exception as e:
        logging.error(
            f"parse data failed:{e}, page={para.page}, catid={para.product_categories}")
    if debug_mode:
        print(records[0])
        return

    error_nums = 0
    for record in records:
        try:
            dataBase.insert_or_update_row(record)
        except Exception as e:
            error_nums += 1
            logging.error(f"write rows failed:{e}")
    logging.info(
        f"write rows success, numbers of rows: {len(records)-error_nums}")

    try:
        dataBase.close()
        pass
    except Exception as e:
        logging.error(f"close mysql connection failed: {e}")


async def main():
    current_date = datetime.date.today()
    ti_range = str(current_date - datetime.timedelta(days=1)).replace("-", "")

    try:
        proxies = getProxies()
    except Exception as e:
        logging.error(f"{e}")

    try:
        response = requests.get(filter_url, headers=headers)
    except Exception as e:
        logging.error(f"get filters failed: {e}")

    try:
        cats = parseCats(response.text)
    except Exception as e:
        logging.error(f"spawn cats subtasks failed: {e}")

    pageTasks = []
    session = aiohttp.ClientSession()

    loop_num = len(cats)
    if debug_mode:
        loop_num = 1
        logging.info(f'1 cat has been spawned in debug mode')
    else:
        logging.info(f'{len(cats)} cats has been spawned')

    for i in range(loop_num):
        para = PageTaskPara(time_range=ti_range, page="1",
                            product_categories=cats[i])
        task = asyncio.create_task(page_task(para, proxies, session))
        pageTasks.append(task)
    try:
        await asyncio.gather(*pageTasks, return_exceptions=True)
    except Exception as e:
        logging.error(f"subtask failed: {e}")
    finally:
        await session.close()

if __name__ == '__main__':
    setup_logging()
    asyncio.run(main())

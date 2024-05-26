import datetime
import logging
import random
import os
import math
import csv
from typing import List
from dataclasses import dataclass
import requests
import asyncio
import aiohttp
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed
import time
from utils.parse import parser, parseCats, parseCatsAndName
from utils.logger import setup_logging
from utils.database import DatabaseManager
from utils.bucket import TokenBucket
from utils.get_env import get_env


filter_url = "https://echotik.live/api/v1/data/videos/leaderboard/filters"


headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7",
    # "authorization": authorization,
    "content-type": "application/json",
    "cookie": "currency=USD; lang=en-US; region=US;",
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
    "x-lang": "en-US",
    "x-region": "US",
}


@dataclass
class TaskInfo:
    interval_low: int
    interval_high: int
    catid: int
    show_case: bool
    total: int


def getProxies() -> List[str]:
    # file_path = proxy_txt
    # with open(file_path, 'r', encoding='utf-8') as f:
    #     proxies = f.readlines()
    # proxies = [p.rstrip('\n') for p in proxies]
    # if len(proxies) == 0:
    #     raise Exception("proxy list not found")
    # return proxies
    return os.environ.get('PROXIES').split(';')


async def parse_list_page(input, catid, show_case):
    data = input['data']
    rows = []
    for d in data:
        influencer_id = d['influencer_id']
        tiktok_id = d['unique_id']
        total_product_cnt = d['total_product_cnt']
        row = [influencer_id, tiktok_id,
               total_product_cnt, catsDic[catid], show_case]
        rows.append(row)
    return await rows


def save_to_csv(rows):
    filename = 'output.csv'
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        for row in rows:
            writer.writerow(row)


async def fetch_list_page(page, per_page, low, high, catid, show_case, session):
    url = 'https://echotik.live/api/v1/data/influencers'
    params = {
        'page': page,
        'per_page': per_page,
        'followers_count': f'{low}-{high}',
        'is_email': 1,
        'order': 'follower_30d_count',
        'sort': 'desc',
        'product_categories': catid
    }
    if show_case:
        params['show_case'] = 1

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7',
        'content-type': 'application/json',
        'dnt': '1',
        'referer': 'https://echotik.live/influencers',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'x-currency': 'USD',
        'x-lang': 'zh-CN',
        'x-region': 'US'
    }
    if env_para['debug_mode']:
        proxy = None
    else:
        proxy = env_para['proxies']
    headers["authorization"] = env_para['auth']
    async with session.get(url, params=params, proxy=proxy, timeout=ClientTimeout(total=10), headers=headers) as response:
        if response.status != 200:
            raise Exception(f"Request failed with status {response.status}")
        data = response.json()
    return await data


def split_interval(lis):
    res = []
    low = lis[0]
    high = lis[1]
    mid = (lis[0]+lis[1])//2
    if low != mid:
        res.append([low, mid])
    if mid+0.5 != high:
        res.append([mid+0.5, high])
    return res


async def worker(queue):
    per_page = 50
    while True:
        task = await queue.get()
        low = task.interval_low
        high = task.interval_high
        cat_id = task.catid
        show_case = task.show_case
        total = task.total
        loop = math.ceil(total / per_page)
        for i in range(loop):
            data = fetch_list_page(i+1, per_page, low, high, cat_id, show_case)
            fields = parse_list_page(data)
            save_to_csv(fields)
        queue.task_done()


async def spawn_tasks(catid: int, show_case: bool, queue: asyncio.Queue, session: aiohttp.ClientSession):
    liss = [[1, 10000000]]
    while True:
        if len(liss) == 0:
            break
        lis = liss.pop()
        total = -1
        try:
            data = fetch_list_page(
                1, 10, lis[0], lis[1], catid, show_case, session)
            total = data['meta']['total']
        except Exception as e:
            print(f'error: {e}')
        if total == -1:
            continue
        if int(total) < 2000:
            t = TaskInfo(interval_low=lis[0], interval_high=lis[1],
                         catid=catid, show_case=show_case, total=total)
            await queue.put(t)
            continue
        if lis[1] - lis[0] > 1:
            liss.extend(split_interval(lis))


async def gather_tasks(cats: list, show_case: bool, queue: asyncio.Queue, session: aiohttp.ClientSession):
    influencerTasks = []
    bucket = TokenBucket(rate=1, capacity=2)

    loop_num = len(cats)
    if env_para['debug_mode']:
        loop_num = 1
        logging.info(f'1 cat has been spawned in debug mode')
    else:
        logging.info(f'{len(cats)} cats has been spawned')

    for i in range(loop_num):
        while not bucket.consume():
            await asyncio.sleep(0.1)
        task = asyncio.create_task(
            spawn_tasks(cats[i], show_case, queue))
        influencerTasks.append(task)

    try:
        await asyncio.gather(*influencerTasks, return_exceptions=True)
    except Exception as e:
        logging.error(f"subtask failed: {e}")
    finally:
        await session.close()


async def main(auth: str):
    try:
        proxies = getProxies()
        logging.info("get proxies")
    except Exception as e:
        logging.error(f"{e}")

    try:
        response = requests.get(filter_url, headers=headers)
        logging.info("get cats resp successful")
    except Exception as e:
        logging.error(f"get filters failed: {e}")

    try:
        cats = parseCats(response.text)
        global catsDic
        catsDic = parseCatsAndName(response.text)
        logging.info("pares cats successful")
    except Exception as e:
        logging.error(f"spawn cats subtasks failed: {e}")

    session = aiohttp.ClientSession()
    queue = asyncio.Queue()
    show_case = False
    asyncio.run(gather_tasks(cats, show_case, queue, session))
    show_case = True
    asyncio.run(gather_tasks(cats, show_case, queue, session))

    nums = 1
    workers = [asyncio.create_task(worker(queue, session))
               for _ in range(nums)]
    await queue.join()
    await asyncio.gather(*workers)


def spider_handler(a, b):
    setup_logging()
    logging.info("set up logging")
    global env_para
    env_para = get_env()
    auth = env_para.get('authorization')
    if auth == '':
        raise Exception('no authorization found')
    logging.info("get auth")
    main(auth)
    return "success"

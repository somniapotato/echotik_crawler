import requests
from parse import parser, parseCats
from dataclasses import dataclass
import requests

filter_url = "https://echotik.live/api/v1/data/videos/leaderboard/filters"

url = "https://echotik.live/api/v1/data/videos/leaderboard/sell-videos"


@dataclass
class PagePara:
    time_type: str = "daily"
    time_range: str
    page: str
    per_page: str = "20"
    product_categories: str


headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7",
    "authorization": "Bearer 107582|PSicPd9hhDfYOoayq054HONngirAFaQd6EDcms2O",
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

# response = requests.get(url, headers=headers, params=params)
# print(response.json())


def main():
    response = requests.get(filter_url, headers=headers)
    cats = parseCats(response.text)
    print(cats)
    para = PagePara(time_range="20240326", page="1",
                    product_categories=cats[0])
    page_params = {
        "time_type": para.time_type,
        "time_range": para.time_range,
        "page": para.page,
        "product_categories": para.product_categories,
        "per_page": para.per_page
    }
    response = requests.get(url, headers=headers, params=page_params)
    res = parser(response.text)
    print(res[0])


if __name__ == '__main__':
    main()

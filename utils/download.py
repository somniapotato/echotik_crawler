import requests
from bs4 import BeautifulSoup

url = 'https://www.tiktok.com//embed//7352954467689532714'
src_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7',
    'dnt': '1',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}
response = requests.get(url, headers=src_headers)

soup = BeautifulSoup(response.text, 'lxml')
video_tag = soup.find('video', attrs={'data-testid': 'play-video-blur'})
video_src = video_tag['src'] if video_tag else None

video_headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,ja;q=0.7',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Range': 'bytes=0-',
    'Referer': 'https://www.tiktok.com/',
    'Sec-Fetch-Dest': 'video',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

response = requests.get(video_src, headers=video_headers)

with open('video.mp4', 'wb') as f:
    f.write(response.content)

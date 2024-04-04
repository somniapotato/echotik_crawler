import json
from dataclasses import dataclass, asdict
from typing import Optional, List
import logging

parserDic = {
    "data": "data",
    "video_id": "video_id",
    "raw_title": "video_title",
    "video_url": "video_url",
    "share_url": "share_url",
    "total_sale_cnt": "total_sale_cnt",  # Video shares
    "views_count": "views_count",  # Views
    "duration": "duration",
    "total_gmv_amt": "total_gmv_amt",
    "publish_time": "publish_time",
    "interact_ratio": "interact_ratio",  # ER Rate
    "digg_count": "digg_count",  # Likes
    "comment_count": "comment_count",  # Comments
    "share_count": "share_count",  # Shares
    "video_products": "video_products",
    "influencer_info": "influencer",
    "influencer_id": "unique_id",
    "influencer_name": "nick_name",
    ####################### related product info #####################
    "product_id": "product_id",
    "product_name": "product_name",
    "raw_category_name": "category_name",
    "cover_url": "cover_url",
    "avg_price": "avg_price",
    "product_total_sale_cnt": "total_sale_cnt",
    "product_total_gmv_amt": "total_gmv_amt",
    "video_sale_cnt": "video_sale_cnt",
    "video_gmv_amt": "video_gmv_amt",
    "N/A": "N/A"
}


@dataclass
class ProductInfo:
    product_id: Optional[int] = None
    product_name: Optional[str] = None
    cat1: Optional[str] = None
    cat2: Optional[str] = None
    cat3: Optional[str] = None
    cover_url: Optional[str] = None
    avg_price: Optional[float] = None
    total_sale_cnt: Optional[int] = None
    total_gmv_amt: Optional[int] = None
    video_sale_cnt: Optional[int] = None
    video_gmv_amt: Optional[int] = None


@dataclass
class VideoData:
    video_id: int
    title: Optional[str] = None
    hashtag: Optional[str] = None  # jsonStr
    video_url: Optional[str] = None
    share_url: Optional[str] = None
    total_sale_cnt: Optional[int] = None
    views_count: Optional[int] = None
    duration: Optional[int] = None  # seconds
    total_gmv_amt: Optional[int] = None
    publish_time: Optional[int] = None  # timestamp
    interact_ratio: Optional[float] = None
    digg_count: Optional[int] = None
    comment_count: Optional[int] = None
    share_count: Optional[int] = None
    video_products: Optional[str] = None  # jsonStr
    influencer_id: Optional[str] = None
    influencer_name: Optional[str] = None


def str2int(s: str):
    if s == "N/A":
        return
    if not isinstance(s, str):
        return
    s = s.replace("$", "")
    if "K" in s:
        s = s.replace("K", "")
        return int(float(s) * 1000)
    if "M" in s:
        s = s.replace("M", "")
        return int(float(s) * 1000000)
    if "s" in s:
        s = s.replace("s", "")
        if "m" in s:
            s = s.replace("m", "")
            return int(s)+60
    return int(s)


def oneRecordParser(data: dict) -> VideoData:
    if parserDic["video_id"] not in data.keys() or data[parserDic["video_id"]] == "":
        raise Exception(f"video id not found, {data}")

    videoData = VideoData(video_id=data[parserDic["video_id"]])

    raw_title = data[parserDic["raw_title"]]
    videoData.title = raw_title.split("#")[0]
    hashtag = raw_title.split("#")[1:]
    videoData.hashtag = json.dumps(list(filter(lambda x: x != "", hashtag)))
    videoData.video_url = data[parserDic["video_url"]]
    videoData.share_url = data[parserDic["share_url"]]

    #### influencer info ###
    influencer_info = data[parserDic["influencer_info"]]
    videoData.influencer_id = influencer_info[parserDic["influencer_id"]]
    videoData.influencer_name = influencer_info[parserDic["influencer_name"]]

    #### video metrics ####
    videoData.total_sale_cnt = str2int(data[parserDic["total_sale_cnt"]])
    videoData.views_count = str2int(data[parserDic["views_count"]])
    if data[parserDic["interact_ratio"]] != parserDic["N/A"]:
        videoData.interact_ratio = float(
            data[parserDic["interact_ratio"]].replace("%", "")) / 100
    videoData.digg_count = str2int(data[parserDic["digg_count"]])
    videoData.comment_count = data[parserDic["comment_count"]]
    videoData.share_count = str2int(data[parserDic["share_count"]])
    videoData.duration = str2int(data[parserDic["duration"]])
    videoData.publish_time = data[parserDic["publish_time"]]

    #### related product info ####
    products: List[ProductInfo] = []
    for i in data[parserDic["video_products"]]:
        p = ProductInfo()
        p.product_id = i[parserDic["product_id"]]
        p.product_name = i[parserDic["product_name"]]
        p.cover_url = i[parserDic["cover_url"]]
        p.total_gmv_amt = i[parserDic["total_gmv_amt"]]
        p.total_sale_cnt = i[parserDic["total_sale_cnt"]]
        p.video_gmv_amt = i[parserDic["video_gmv_amt"]]
        p.video_sale_cnt = i[parserDic["video_sale_cnt"]]
        if i[parserDic["avg_price"]] != parserDic["N/A"]:
            p.avg_price = float(i[parserDic["avg_price"]].replace("$", ""))
        p.cat1 = i[parserDic["raw_category_name"]]
        products.append(asdict(p))

    videoData.video_products = json.dumps(products)
    return videoData


def parser(rawJson: str) -> list:
    data = json.loads(rawJson)[parserDic["data"]]
    res: List[VideoData] = []
    for each_data in data:
        try:
            parsed_data = oneRecordParser(each_data)
            res.append(parsed_data)
        except Exception as e:
            logging.error(f"parse video failed:{e}")
    return res


def parseCats(catsJson: str) -> list:
    res = []
    catsData = json.loads(catsJson)
    for i in catsData["data"]["product_categories"]:
        if i["id"] != "":
            res.append(i["id"])
    return res


def test():
    file_path = 'test.data'
    with open(file_path, 'r', encoding='utf-8') as f:
        test_data = f.read()
    print(test_data)
    res = parser(test_data)
    print(len(res))


if __name__ == '__main__':
    test()

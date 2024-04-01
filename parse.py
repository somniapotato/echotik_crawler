import json
from dataclasses import dataclass, asdict
from typing import Optional, List, Type

parserDic = {
    "data": "data",
    "raw_title": "video_title",
    "video_url": "video_url",
    "total_sale_cnt": "total_sale_cnt",  # 视频销量
    "views_count": "views_count",  # 播放
    "duration": "duration",
    "total_gmv_amt": "total_gmv_amt",
    "publish_time": "publish_time",
    "interact_ratio": "interact_ratio",  # ER 互动率
    "digg_count": "digg_count",  # 点赞
    "comment_count": "comment_count",  # 评论
    "share_count": "share_count",  # 分享
    "video_products": "video_products",
    ####################### product info #####################
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
    video_id: Optional[int] = None
    title: Optional[str] = None
    hashtag: Optional[str] = None  # jsonStr
    video_url: Optional[str] = None
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
    return int(s)


def oneRecordParser(data: dict) -> VideoData:
    videoData = VideoData()
    raw_title = data[parserDic["raw_title"]]
    videoData.title = raw_title.split("#")[0]
    videoData.hashtag = json.dumps(raw_title.split("#")[1:])
    videoData.video_url = data[parserDic["video_url"]]

    #### video metrics ####
    videoData.total_sale_cnt = str2int(data[parserDic["total_sale_cnt"]])
    videoData.views_count = str2int(data[parserDic["views_count"]])
    if data[parserDic["interact_ratio"]] != parserDic["N/A"]:
        videoData.interact_ratio = float(
            data[parserDic["interact_ratio"]].replace("%", "")) / 100
    videoData.digg_count = str2int(data[parserDic["digg_count"]])
    videoData.comment_count = str2int(data[parserDic["comment_count"]])
    videoData.share_count = str2int(data[parserDic["share_count"]])

    #### product info ####
    products: List[ProductInfo] = []
    for i in data[parserDic["video_products"]]:
        p = ProductInfo()
        p.product_id = i[parserDic["product_id"]]
        p.product_name = i[parserDic["product_name"]]
        p.cover_url = i[parserDic["cover_url"]]
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
        res.append(oneRecordParser(each_data))
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
    # print(test_data)
    res = parser(test_data)
    print(res[0])


if __name__ == '__main__':
    test()

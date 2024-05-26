import json
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple
import logging
import uuid


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
    ####################### related influencer info #####################
    "influencer_info": "influencer",
    "influencer_id": "unique_id",
    "influencer_name": "nick_name",
    "followers": "follower_count",
    ####################### related product info #####################
    "product_id": "product_id",
    "product_name": "product_name",
    "category_name": "category_name",
    "cover_url": "cover_url",
    "avg_price": "avg_price",
    "product_total_sale_cnt": "total_sale_cnt",
    "product_total_gmv_amt": "total_gmv_amt",
    "video_sale_cnt": "video_sale_cnt",
    "video_gmv_amt": "video_gmv_amt",
    "N/A": "N/A"
}

PLATFORM = {
    "echotik": 1
}


@dataclass
class VideoMeta:
    uuid: str
    video_id: str
    influencer_id: Optional[str] = None
    category: Optional[str] = None
    video_title: Optional[str] = None
    hashtag: Optional[str] = None
    video_url: Optional[str] = None
    share_url: Optional[str] = None
    duration: Optional[int] = None  # seconds
    publish_time: Optional[int] = None  # yy-mm-dd
    product_id: Optional[str] = None  # List[str]
    video_text: Optional[int] = None
    platform: Optional[int] = None


@dataclass
class VideoTrendy:
    uuid: str
    video_id: str
    influencer_id: Optional[str] = None
    date: Optional[str] = None
    sales: Optional[int] = None
    views: Optional[int] = None
    er_ratio: Optional[float] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    total_gmv_amt: Optional[int] = None
    platform: Optional[int] = None


@dataclass
class Influencer:
    uuid: str
    influencer_id: str
    date: Optional[str] = None
    follower_count: Optional[int] = None
    platform: Optional[int] = None


@dataclass
class ProductInfo:
    uuid: str
    product_id: Optional[int] = None
    date: Optional[str] = None
    product_name: Optional[str] = None
    category: Optional[str] = None
    cover_url: Optional[str] = None
    avg_price: Optional[float] = None
    total_sale_cnt: Optional[int] = None
    total_gmv_amt: Optional[int] = None
    video_sale_cnt: Optional[int] = None
    video_gmv_amt: Optional[int] = None
    platform: Optional[int] = None


def safe_execute(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"Error executing {func.__name__}: {e}")
        return None


def str2int(s: str):
    try:
        if s == "N/A" or s == "-":
            return
        if not isinstance(s, str):
            return int(float(s))
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
            return int(float(s))+60
        return int(float(s))
    except Exception as e:
        logging.error(f"str 2 int failed, meta data:{s}, error: {e}")


def video_meta_parer(data: dict) -> VideoMeta:
    if parserDic["video_id"] not in data.keys() or data[parserDic["video_id"]] == "":
        raise Exception(f"video id not found, {data}")
    video_meta = VideoMeta(
        uuid=str(uuid.uuid4()), video_id=data[parserDic["video_id"]])
    video_meta.category = data[
        parserDic["video_products"]][0]["category_name"]
    raw_title = data[parserDic["raw_title"]]
    video_meta.video_title = raw_title.split("#")[0][:500]
    hashtag = raw_title.split("#")[1:]
    video_meta.hashtag = json.dumps(list(filter(lambda x: x != "", hashtag)))
    video_meta.video_url = data[parserDic["video_url"]]
    video_meta.share_url = data[parserDic["share_url"]]
    video_meta.duration = str2int(data[parserDic["duration"]])
    video_meta.publish_time = data[parserDic["publish_time"]]
    video_meta.influencer_id = data[
        parserDic["influencer_info"]][parserDic["influencer_id"]]
    product_id_lis = []
    for i in data[parserDic["video_products"]]:
        product_id_lis.append(i["product_id"])
    video_meta.product_id = json.dumps(product_id_lis)
    video_meta.platform = PLATFORM['echotik']
    return video_meta


def video_trendy_parer(data: dict, date: str) -> VideoTrendy:
    if parserDic["video_id"] not in data.keys() or data[parserDic["video_id"]] == "":
        raise Exception(f"video id not found, {data}")
    video_trendy = VideoTrendy(
        uuid=str(uuid.uuid4()), video_id=data[parserDic["video_id"]])
    video_trendy.influencer_id = data[
        parserDic["influencer_info"]][parserDic["influencer_id"]]
    video_trendy.date = date
    video_trendy.sales = str2int(data[parserDic["total_sale_cnt"]])
    video_trendy.views = str2int(data[parserDic["views_count"]])
    try:
        video_trendy.er_ratio = round(float(
            data[parserDic["interact_ratio"]].replace("%", "")) / 100, 2)
    except Exception as e:
        logging.error(
            f'interact_ratio meta data:{data[parserDic["interact_ratio"]]}, error:{e}')
    video_trendy.likes = str2int(data[parserDic["digg_count"]])
    video_trendy.comments = str2int(data[parserDic["comment_count"]])
    video_trendy.sales = str2int(data[parserDic["total_sale_cnt"]])
    video_trendy.total_gmv_amt = str2int(data[parserDic["total_gmv_amt"]])
    video_trendy.platform = PLATFORM['echotik']
    return video_trendy


def influencer_parer(data: dict, date: str) -> Influencer:

    influencer_info = data[parserDic["influencer_info"]]
    influencer = Influencer(
        uuid=str(uuid.uuid4()), influencer_id=influencer_info[parserDic["influencer_id"]])
    influencer.date = date
    influencer.follower_count = str2int(
        influencer_info[parserDic["followers"]])
    influencer.platform = PLATFORM['echotik']
    return influencer


def product_info_parer(data: dict, date: str) -> List[ProductInfo]:

    products: List[ProductInfo] = []
    for i in data[parserDic["video_products"]]:
        p = ProductInfo(uuid=str(uuid.uuid4()))
        p.product_id = i[parserDic["product_id"]]
        p.date = date
        p.product_name = i[parserDic["product_name"]]
        p.category = i[parserDic["category_name"]]
        p.cover_url = i[parserDic["cover_url"]].split('?')[0]
        p.total_gmv_amt = str2int(i[parserDic["total_gmv_amt"]])
        p.total_sale_cnt = str2int(i[parserDic["total_sale_cnt"]])
        p.video_gmv_amt = str2int(i[parserDic["video_gmv_amt"]])
        p.video_sale_cnt = str2int(i[parserDic["video_sale_cnt"]])
        p.platform = PLATFORM['echotik']
        if i[parserDic["avg_price"]] != parserDic["N/A"]:
            try:
                p.avg_price = float(i[parserDic["avg_price"]].replace("$", ""))
            except Exception as e:
                logging.error(
                    f'avg_price meta data:{data[parserDic["avg_price"]]}, error:{e}')
        products.append(p)
    return products


def parser(rawJson: str, date: str) -> Tuple[List[VideoMeta], List[VideoTrendy], List[Influencer], List[ProductInfo]]:
    data = json.loads(rawJson)[parserDic["data"]]
    video_meta: List[VideoMeta] = []
    video_trendy: List[VideoTrendy] = []
    influencer: List[VideoTrendy] = []
    products_info: List[ProductInfo] = []
    for each_data in data:
        try:
            parsed_video_meta = video_meta_parer(each_data)
            video_meta.append(parsed_video_meta)
        except Exception as e:
            logging.error(f"parse video info failed:{e}, data:{each_data}")
        try:
            parsed_video_trendy = video_trendy_parer(each_data, date)
            video_trendy.append(parsed_video_trendy)
        except Exception as e:
            logging.error(f"parse video trendy failed:{e}, data:{each_data}")
        try:
            parsed_influencer = influencer_parer(each_data, date)
            influencer.append(parsed_influencer)
        except Exception as e:
            logging.error(f"parse influencer failed:{e}, data:{each_data}")
        try:
            parsed_product_info_lis = product_info_parer(each_data, date)
            products_info.extend(parsed_product_info_lis)
        except Exception as e:
            logging.error(f"parse product info failed:{e}, data:{each_data}")

    return video_meta, video_trendy, influencer, products_info


def parseCats(catsJson: str) -> list:
    res = []
    catsData = json.loads(catsJson)
    for i in catsData["data"]["product_categories"]:
        if i["id"] != "":
            res.append(i["id"])
    return res


def parseCatsAndName(catsJson: str) -> dict:
    res = {}
    catsData = json.loads(catsJson)
    for i in catsData["data"]["product_categories"]:
        if i["id"] != "":
            res[i["id"]] = i["name"]
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

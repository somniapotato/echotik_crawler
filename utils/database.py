import logging
from typing import List
import asyncio
import psycopg2
from utils.parse import VideoMeta, VideoTrendy, Influencer, ProductInfo


class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=database
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    async def insert_metadata(self, data: VideoMeta):
        try:
            insert_query = """
            INSERT INTO video_metadata (uuid, video_id, category, video_title, hash_tags, video_url, share_url, duration, publish_time, influencer_id, product_id, video_text, platform) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            record_to_insert = (data.uuid, data.video_id, data.category, data.video_title, data.hashtag, data.video_url,
                                data.share_url, data.duration, data.publish_time, data.influencer_id, data.product_id, data.video_text, data.platform)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()
        except psycopg2.Error as e:
            logging.error(f"insert meta table failed: {e}, data:{data}")
            self.connection.rollback()

    async def insert_trendy(self, data: VideoTrendy):
        try:
            insert_query = """
            INSERT INTO video_trendy (uuid, date, video_id, sales, views, er_ratio, likes, comments, total_gmv_amt, platform) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.date, data.video_id, data.sales, data.views,
                                data.er_ratio, data.likes, data.comments, data.total_gmv_amt, data.platform)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert trendy table failed: {e}, data:{data}")
            self.connection.rollback()

    async def insert_influencer(self, data: Influencer):
        try:
            insert_query = """
            INSERT INTO influencer (uuid, date, influencer_id, follower_count, platform) 
            VALUES (%s, %s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.date,
                                data.influencer_id, data.follower_count, data.platform)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert influencer table failed: {e}, data:{data}")
            self.connection.rollback()

    async def insert_product(self, data: ProductInfo):
        try:
            insert_query = """
            INSERT INTO product_info (uuid, product_id, date, product_name, category, cover_url, avg_price, total_sale_cnt, total_gmv_amt, video_sale_cnt, video_gmv_amt, platform) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.product_id, data.date, data.product_name, data.category, data.cover_url,
                                data.avg_price, data.total_sale_cnt, data.total_gmv_amt, data.video_sale_cnt, data.video_gmv_amt, data.platform)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert product table failed: {e}, data:{data}")
            self.connection.rollback()


if __name__ == '__main__':
    pass

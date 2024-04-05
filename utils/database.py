import psycopg2
from utils.parse import VideoMeta, VideoTrendy, Influencer, ProductInfo
from typing import List
import json
import logging


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

    def insert_metadata(self, data: VideoMeta):
        try:
            insert_query = """
            INSERT INTO video_metadata (uuid, video_id, category, video_title, video_url, share_url, duration, publish_time, influencer_id, product_id, video_text) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            record_to_insert = (data.uuid, data.video_id, data.category, data.video_title, data.video_url,
                                data.share_url, data.duration, data.publish_time, data.influencer_id, data.product_id, data.video_text)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()
        except psycopg2.Error as e:
            logging.error(f"insert meta table failed: {e}")

    def insert_trendy(self, data: VideoTrendy):
        try:
            insert_query = """
            INSERT INTO video_trendy (uuid, date, video_id, sales, views, er_ratio, likes, comments, digg_count, total_gmv_amt) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.date, data.video_id, data.sales, data.views,
                                data.er_ratio, data.likes, data.comments, data.digg_count, data.total_gmv_amt)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert meta table failed: {e}")

    def insert_influencer(self, data: Influencer):
        try:
            insert_query = """
            INSERT INTO influencer (uuid, date, influencer_id, follower_count) 
            VALUES (%s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.date,
                                data.influencer_id, data.follower_count)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert meta table failed: {e}")

    def insert_product(self, data: ProductInfo):
        try:
            insert_query = """
            INSERT INTO product_info (uuid, product_id, date, product_name, category, cover_url, avg_price, total_sale_cnt, total_gmv_amt, video_sale_cnt, video_gmv_amt) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            record_to_insert = (data.uuid, data.product_id, data.date, data.product_name, data.category, data.cover_url,
                                data.avg_price, data.total_sale_cnt, data.total_gmv_amt, data.video_sale_cnt, data.video_gmv_amt)

            self.cursor.execute(insert_query, record_to_insert)
            self.connection.commit()

        except psycopg2.Error as e:
            logging.error(f"insert meta table failed: {e}")


def test_data_base(data):
    HOST = "localhost"
    USER = "postgres"
    PASSWORD = "666666"
    DATABASE = "trendy"
    dataBase = DatabaseManager(HOST, USER, PASSWORD, DATABASE)

    dataBase.insert_metadata(data)
    # file_path = 'test2.data'
    # with open(file_path, 'r', encoding='utf-8') as f:
    #     data = json.load(f)
    # test_data = VideoData(**data)

    # dataBase.insert_or_update_row(test_data)
    dataBase.close()


if __name__ == '__main__':
    pass

import mysql.connector
from utils.parse import VideoData
# from parse import VideoData
from typing import List
import json


class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def insert_or_update_row(self, data: VideoData):
        # print(data)
        try:
            sql = """
            INSERT INTO echotik_videos (video_id, title, video_url, share_url, sales, views, er_ratio, comments, shares, influencer_id, influencer_name, product_info, ctime, mtime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())
            ON DUPLICATE KEY UPDATE 
                video_id=VALUES(video_id), 
                title=VALUES(title), 
                video_url=VALUES(video_url), 
                share_url=VALUES(share_url),
                sales=VALUES(sales), 
                views=VALUES(views), 
                er_ratio=VALUES(er_ratio), 
                comments=VALUES(comments), 
                shares=VALUES(shares), 
                influencer_id=VALUES(influencer_id), 
                influencer_name=VALUES(influencer_name), 
                product_info=VALUES(product_info), 
                mtime=UNIX_TIMESTAMP();
            """
            self.cursor.execute(sql, (data.video_id, data.title, data.video_url, data.share_url, data.total_sale_cnt,
                                      data.views_count, data.interact_ratio, data.comment_count, data.share_count, data.influencer_id, data.influencer_name, data.video_products))
            self.connection.commit()

        except mysql.connector.Error as error:
            print("Failed to insert record into videos table {}".format(error))

    def write_rows(self, rows: List[VideoData]):
        print(rows[0])
        for row in rows:
            print(row)
            self.insert_or_update_row(row)


def test():
    HOST = "localhost"
    USER = "root"
    PASSWORD = "123456"
    DATABASE = "videos"
    dataBase = DatabaseManager(HOST, USER, PASSWORD, DATABASE)

    file_path = 'test2.data'
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(data)
    test_data = VideoData(**data)

    dataBase.insert_or_update_row(test_data)
    dataBase.close()


if __name__ == '__main__':
    test()

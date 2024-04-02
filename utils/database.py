import mysql.connector
from parse import VideoData


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
        try:
            sql = """
            INSERT INTO videos (video_id, title, video_url, sales, views, er_ratio, comments, shares, product_info, ctime, mtime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())
            ON DUPLICATE KEY UPDATE 
                title=VALUES(title), 
                video_url=VALUES(video_url), 
                sales=VALUES(sales), 
                views=VALUES(views), 
                er_ratio=VALUES(er_ratio), 
                comments=VALUES(comments), 
                shares=VALUES(shares), 
                product_info=VALUES(product_info), 
                mtime=UNIX_TIMESTAMP();
            """

            self.cursor.execute(sql, (data.video_id, data.title, data.video_url, data.total_sale_cnt,
                                      data.views_count, data.interact_ratio, data.comment_count, data.share_count, data.video_products))

            self.connection.commit()

        except mysql.connector.Error as error:
            print("Failed to insert record into videos table {}".format(error))


def test():
    HOST = "localhost"
    USER = "ROOT"
    PASSWORD = "yinghua13"
    DATABASE = "echotik_crawler"
    dataBase = DatabaseManager(HOST, USER, PASSWORD, DATABASE)
    dataBase.insert_or_update_row(
        "1234", "who am i", "url", 123, 123, 0, 1, 123, 123, "info")
    dataBase.close()


if __name__ == '__main__':
    test()

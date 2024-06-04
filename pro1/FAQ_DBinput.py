import pymysql
import pandas as pd
import numpy as np

class DatabaseHandler:
    def __init__(self, host, user, password, db, charset='utf8'):
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
        self.cur = self.conn.cursor()

    def create_table(self, create_table_sql):
        self.cur.execute(create_table_sql)

    def insert_data(self, insert_sql, data):
        self.cur.executemany(insert_sql, data)
        
    def read_csv(self, file_path, encoding='utf8'):
        df = pd.read_csv(file_path, encoding=encoding)
        df = df.replace({np.nan: None})
        return df

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()

# 메인 코드
db_handler = DatabaseHandler(host='127.0.0.1', user='root', password='1234', db='SK_rentcar_faq')


# SK_rentcar_FAQ_TBL 테이블 생성
SK_FAQ_table_sql = """
CREATE TABLE IF NOT EXISTS SK_FAQ_TBL ( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    Question VARCHAR(255),
    Answer VARCHAR(1000)
)
"""
db_handler.create_table(SK_FAQ_table_sql)

# SK_rentcar_TBL 데이터 삽입
SK_FAQ_df = db_handler.read_csv('C:/ex/pro1/db/faq_df.csv')
SK_FAQ_data = SK_FAQ_df[['Question', 'Answer']].values.tolist()
SK_FAQ_insert_sql = """INSERT INTO SK_FAQ_TBL (Question, Answer) VALUES (%s, %s)"""
db_handler.insert_data(SK_FAQ_insert_sql, SK_FAQ_data)

# 데이터베이스 커밋 및 연결 종료
db_handler.commit_and_close()
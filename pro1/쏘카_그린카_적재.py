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
        
    def read_csv(self, file_path, encoding='cp949'):
        df = pd.read_csv(file_path, encoding=encoding)
        df = df.replace({np.nan: None})
        return df

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()

# 메인 코드
db_handler = DatabaseHandler(host='127.0.0.1', user='root', password='1234', db='socar_greencar_tbl')

# socar_greencar_TBL 테이블 생성
socar_greencar_table_sql = """
CREATE TABLE IF NOT EXISTS socar_greencar_TBL ( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    zoneId VARCHAR(255),
    zoneName VARCHAR(255),
    address VARCHAR(255), 
    latitude FLOAT, 
    longitude FLOAT, 
    car_type INT
)
"""
db_handler.create_table(socar_greencar_table_sql)

# socar_greencar_TBL 데이터 삽입
socar_greencar_df = db_handler.read_csv('C:/ex/pro1/db/car_sharing_df.csv')
socar_greencar_data = socar_greencar_df[['zoneId', 'zoneName', 'address', 'latitude', 'longitude', 'car_type']].values.tolist()
socar_greencar_insert_sql = """INSERT INTO socar_greencar_TBL (`zoneId`, `zoneName`, `address`, `latitude`, `longitude`, `car_type`) VALUES (%s, %s, %s, %s, %s, %s)"""
db_handler.insert_data(socar_greencar_insert_sql, socar_greencar_data)

# 데이터베이스 커밋 및 연결 종료
db_handler.commit_and_close()

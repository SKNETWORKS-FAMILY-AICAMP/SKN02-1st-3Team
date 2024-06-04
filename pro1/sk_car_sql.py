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
db_handler = DatabaseHandler(host='127.0.0.1', user='root', password='1234', db='skrentcardb')

# locationTBL 테이블 생성
location_table_sql = """
CREATE TABLE IF NOT EXISTS locationTBL (
    id INT AUTO_INCREMENT PRIMARY KEY, 
    `업체명` VARCHAR(255), 
    `위도` FLOAT, 
    `경도` FLOAT, 
    `제공기관명` VARCHAR(255)
)
"""
db_handler.create_table(location_table_sql)

# locationTBL 데이터 삽입
location_df = db_handler.read_csv('C:/ex/pro1/db/location.csv')
location_data = location_df[['업체명', '위도', '경도', '제공기관명']].values.tolist()
location_insert_sql = """INSERT INTO locationTBL (`업체명`, `위도`, `경도`, `제공기관명`) VALUES (%s, %s, %s, %s)"""
db_handler.insert_data(location_insert_sql, location_data)

# countTBL 테이블 생성
count_table_sql = """
CREATE TABLE IF NOT EXISTS countTBL (
    id INT AUTO_INCREMENT, 
    `자동차총보유대수` INT, 
    `승용차보유대수` INT, 
    `승합차보유대수` INT, 
    `전기승용자동차보유대수` INT,
    `전기승합자동차보유대수` INT,
    FOREIGN KEY (id) REFERENCES locationTBL(id)
)
"""
db_handler.create_table(count_table_sql)

# countTBL 데이터 삽입
count_df = db_handler.read_csv('C:/ex/pro1/db/count.csv')
count_data = count_df[['자동차총보유대수', '승용차보유대수', '승합차보유대수', '전기승용자동차보유대수', '전기승합자동차보유대수']].values.tolist()
count_insert_sql = """INSERT INTO countTBL (`자동차총보유대수`, `승용차보유대수`, `승합차보유대수`, `전기승용자동차보유대수`, `전기승합자동차보유대수`) VALUES (%s, %s, %s, %s, %s)"""
db_handler.insert_data(count_insert_sql, count_data)

# timeTBL 테이블 생성
time_table_sql = """
CREATE TABLE IF NOT EXISTS timeTBL (
    id INT AUTO_INCREMENT, 
    `평일운영시작시각` TIME, 
    `평일운영종료시각` TIME, 
    `주말운영시작시각` TIME, 
    `주말운영종료시각` TIME,
    `공휴일운영시작시각` TIME,
    `공휴일운영종료시각` TIME,
    `휴무일` CHAR(5),
    FOREIGN KEY (id) REFERENCES locationTBL(id)
)
"""
db_handler.create_table(time_table_sql)

# timeTBL 데이터 삽입
time_df = db_handler.read_csv('C:/ex/pro1/db/time.csv')
time_data = time_df[['평일운영시작시각', '평일운영종료시각', '주말운영시작시각', '주말운영종료시각', '공휴일운영시작시각', '공휴일운영종료시각', '휴무일']].values.tolist()
time_insert_sql = """INSERT INTO timeTBL (`평일운영시작시각`, `평일운영종료시각`, `주말운영시작시각`, `주말운영종료시각`, `공휴일운영시작시각`, `공휴일운영종료시각`, `휴무일`) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
db_handler.insert_data(time_insert_sql, time_data)

# 데이터베이스 커밋 및 연결 종료
db_handler.commit_and_close()

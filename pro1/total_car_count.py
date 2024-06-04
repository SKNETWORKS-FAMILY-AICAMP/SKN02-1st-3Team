# Python 전처리

import pandas as pd
import re

df = pd.read_csv('C:/ex/pro1/db/car_share.csv', encoding='cp949')

df['업체명'] = df['업체명'].str.replace('에스케이','SK')
df['업체명'] = df['업체명'].str.replace('㈜','')

# 정규표현식 패턴
pattern = r'\(.*?\)'

# 정규표현식을 이용하여 괄호와 괄호 안의 문자 선택 및 제거하는 함수
def remove_brackets_and_text_inside(text):
    return re.sub(pattern, '', text)

# 특정 열에 함수 적용하여 새로운 열 생성
df['업체명'] = df['업체명'].apply(remove_brackets_and_text_inside)

# '쏘카'가 포함된 값을 '쏘카'로 통합
df['업체명'] = df['업체명'].apply(lambda x: '쏘카' if '쏘카' in x else x)

# 'SK'가 포함된 값을 'SK'로 통합
df['업체명'] = df['업체명'].apply(lambda x: 'SK' if 'SK' in x else x)

# 컬럼명 rename
df = df.rename(columns={'업체명':'name', '자동차총보유대수':'car_count'})

# 필요한 컬럼만 추출
sub_df = df[['name', 'car_count']]

# 중복 제거
final_df = sub_df.drop_duplicates()

final_df.to_csv('C:/ex/pro1/db/total_rentcar.csv', encoding='cp949', index=False)

#################################################

# Mysql 적재

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
db_handler = DatabaseHandler(host='127.0.0.1', user='root', password='1234', db='total_rentcar_db')

# total_rentcar_TBL 테이블 생성
total_rentcar_table_sql = """
CREATE TABLE IF NOT EXISTS total_rentcar_TBL ( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    car_count INT
)
"""
db_handler.create_table(total_rentcar_table_sql)

# total_rentcar_TBL 데이터 삽입
total_rentcar_df = db_handler.read_csv('C:/ex/pro1/db/total_rentcar.csv')
total_rentcar_data = total_rentcar_df[['name', 'car_count']].values.tolist()
total_rentcar_insert_sql = """INSERT INTO total_rentcar_TBL (`name`, `car_count`) VALUES (%s, %s)"""
db_handler.insert_data(total_rentcar_insert_sql, total_rentcar_data)

# 데이터베이스 커밋 및 연결 종료
db_handler.commit_and_close()


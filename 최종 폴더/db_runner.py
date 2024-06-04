# csv 파일을 데이터베이스로 저장
import pymysql
import pandas as pd
import numpy as np
import os

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


if __name__ == '__main__':
    path = os.path.join(os.getcwd(), 'result')
    
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
    location_df = db_handler.read_csv(os.path_join(path, 'location.csv'))
    location_data = location_df[['업체명', '위도', '경도', '제공기관명']].values.tolist()
    location_insert_sql = """INSERT INTO locationTBL (`업체명`, `위도`, `경도`, `제공기관명`) VALUES (%s, %s, %s, %s)"""
    db_handler.insert_data(location_insert_sql, location_data)

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
    total_rentcar_df = db_handler.read_csv('C:/Users/user/Desktop/yooob/sk networks/프로젝트/자동차데이터셋/total_rentcar.csv')
    total_rentcar_data = total_rentcar_df[['name', 'car_count']].values.tolist()
    total_rentcar_insert_sql = """INSERT INTO total_rentcar_TBL (`name`, `car_count`) VALUES (%s, %s)"""
    db_handler.insert_data(total_rentcar_insert_sql, total_rentcar_data)
    
    # 데이터베이스 커밋 및 연결 종료
    db_handler.commit_and_close()

    print(" >> 데이터베이스 적재 완료")
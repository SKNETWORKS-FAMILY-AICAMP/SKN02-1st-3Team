import pandas as pd
import re
import os

## 전처리 및 중복 제거
class RentalCarDataProcessor:
    def __init__(self, input_file_path, encoding='cp949'):
        self.df = pd.read_csv(input_file_path, encoding=encoding)

    def preprocess_data_sk_map(self):
        self.df['업체명'] = self.df['업체명'].str.replace('에스케이', 'SK') # '에스케이' => 'SK'
        self.df = self.df[self.df['업체명'].str.contains('SK')] # 'SK' 문자열 포함 업체만 추출
        self.df['업체명'] = self.df['업체명'].str.replace('㈜', '') # '㈜' 라는 특수문자 제거
        self.df['업체명'] = self.df['업체명'].apply(self.remove_brackets_and_text_inside) # 아래 함수 참조
        self.df = self.df.drop_duplicates(['위도', '경도']) # '위도', '경도' 기준으로 중복 제거 즉, 위치가 같은 업체가 없게!
        return self.df

    def preprocess_data_total_rentcar(self):
        self.df['업체명'] = self.df['업체명'].apply(self.remove_brackets_and_text_inside) # 아래 함수 참조
        self.df['업체명'] = self.df['업체명'].str.replace('㈜', '') # '㈜' 라는 특수문자 제거
        self.df['업체명'] = self.df['업체명'].str.replace('에스케이', 'SK') # '에스케이' => 'SK'
        self.df['업체명'] = self.df['업체명'].apply(lambda x: '쏘카' if '쏘카' in x else x)
        self.df['업체명'] = self.df['업체명'].apply(lambda x: 'SK' if 'SK' in x else x)
        self.df = self.df.rename(columns={'업체명':'name', '자동차총보유대수':'car_count'})
        self.df = self.df[['name', 'car_count']]
        self.df = self.df.drop_duplicates()
        return self.df

    @staticmethod  # 데코레이터 사용하여 remove_brackets_and_text_inside() 정적 메서드로 정의
    def remove_brackets_and_text_inside(text):
        pattern = r'\(.*?\)' # 괄호와 괄호 안에 문자까지 제거
        return re.sub(pattern, '', text)

    def save_to_csv(self, df, output_file_path, encoding='cp949'):
        df.to_csv(output_file_path, encoding=encoding, index=False)

    def get_shape(self):
        return self.df.shape


def get_rentcar_info():
    # 데이터 저장 경로 설정
    data_path = os.path.join(os.getcwd(), 'data')
    result_path = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_path, exist_ok=True)

    ############## 1. 데이터 전처리
    input_file_path = os.path.join(data_path, '전국렌터카업체정보표준데이터.csv')
    output_file_path1 = os.path.join(result_path, 'sk_rentcar_df.csv')
    output_file_path2 = os.path.join(result_path, 'total_rentcar.csv')

    # RentalCarDataProcessor 인스턴스 생성
    processor = RentalCarDataProcessor(input_file_path)

    # 데이터 전처리
    sk_rentcar_df = processor.preprocess_data_sk_map()
    total_rentcar = processor.preprocess_data_total_rentcar()

    # # 데이터 크기 출력
    # print("데이터 크기:", processor.get_shape())

    # CSV 파일로 저장
    processor.save_to_csv(sk_rentcar_df, output_file_path1)
    processor.save_to_csv(total_rentcar, output_file_path2)
    
    ############## 2. 데이터프레임 분할
    location_df = sk_rentcar_df[['업체명','위도','경도','제공기관명']]
    output_file_path3 = os.path.join(result_path, 'location.csv')
    location_df.to_csv(os.path.join(output_file_path3), encoding='cp949', index=False)

    print(" >> 데이터 저장 완료")
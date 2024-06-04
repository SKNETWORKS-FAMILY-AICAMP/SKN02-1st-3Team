from crawling.car_sharing import get_car_sharing
from crawling.rentcar_info import get_rentcar_info
from crawling.sk_rentcar_faq import get_faq

if __name__ == '__main__':
    print("====================== [렌트카 정보] 가져오기 및 전처리 중 ======================")
    get_rentcar_info()
    
    print("====================== [카셰어링] 크롤링 중 ======================")
    get_car_sharing()
    
    print("====================== [FAQ] 크롤링 중 ======================")
    get_faq()
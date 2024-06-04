import os
import datetime
import json
import pandas as pd

import urllib.request
from urllib.parse import quote

class CarSharing:
    def __init__(self, service_url, ServiceKey, address_list, numOfRows, pageNo):
        self.service_url = service_url
        self.ServiceKey = ServiceKey
        self.address_list = address_list
        self.numOfRows = numOfRows
        self.pageNo = pageNo

    def getRequestUrl(self, url):
        req = urllib.request.Request(url)    
        try: 
            response = urllib.request.urlopen(req)
            if response.getcode() == 200:
                print ("[%s] Url Request Success" % datetime.datetime.now())
                return response.read().decode('utf-8')
        except Exception as e:
            print(e)
            print("[%s] Error for URL : %s" % (datetime.datetime.now(), url))
            return None
            
    def getCarSharingItem(self, address):
        address = quote(address)
        service_url = self.service_url
        parameters = "?_type=json&serviceKey=" + self.ServiceKey   #인증키
        parameters += "&zoneAddr=" + address
        parameters += "&numOfRows=" + str(self.numOfRows)
        parameters += "&pageNo=" + str(self.pageNo)
        url = service_url + parameters

        retData = self.getRequestUrl(url)
        
        # print("Response Data:", retData)
        
        if (retData == None):
            return None
        else:
            return json.loads(retData)
        
    def getCarSharingService(self): #ServiceKey, address_list, numOfRows, pageNo):
        CarSharingResult = []    
        isDataEnd = 0 #데이터 끝 확인용 flag 초기화    
        
        for address in self.address_list:        
            if(isDataEnd == 1): break #데이터 끝 flag 설정되어있으면 작업 중지.    
            jsonData = self.getCarSharingItem(address) #[CODE 2]
            
            if (jsonData['response']['header']['resultMsg'] == 'NORMAL SERVICE.'):               
                # 입력된 범위까지 수집하지 않았지만, 더이상 제공되는 데이터가 없는 마지막 항목인 경우 -------------------
                if jsonData['response']['body']['items'] == '': 
                    isDataEnd = 1 #데이터 끝 flag 설정
                    print("데이터 없음.... \n")                    
                    break                

                for item in jsonData['response']['body']['items']['item']:
                    address = item['address']
                    latitude = item['latitude']
                    longitude = item['longitude']
                    car_type = item['type']
                    zoneId = item['zoneId']
                    zoneName = item['zoneName']
                    CarSharingResult.append({'address': address, 'latitude': latitude,
                                        'longitude': longitude, 'car_type': car_type,
                                        'zoneId': zoneId, 'zoneName': zoneName,})
                print('----------------------------------------------------------------------')                
                    
        return CarSharingResult

def get_car_sharing():
    service_url = "http://apis.data.go.kr/1613000/CarSharingInfoService/getCarZoneListByAddr"
    ServiceKey = "tQsECUqWLM2JML%2Bnz3kOEq9uDjAED9izGBtf%2FmMuGV7J7umjp9e2THwU2XkZAeos6e11mz0peBAEaQMGtFfBcA%3D%3D"
    address_list = ['강원', '경기', '경남', '경북', '광주', '대구', '대전', '부산', '서울','울산', '인천', '전남', '전북', '제주', '충남', '충북']
    numOfRows, pageNo = 1500, 1
    
    car_sharing_object = CarSharing(service_url, ServiceKey, address_list, numOfRows, pageNo)
    CarSharingResult = car_sharing_object.getCarSharingService()

    # 파일 저장
    result_path = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_path, exist_ok=True)

    columns = ["address", "latitude", "longitude", "car_type", "zoneId", "zoneName"]
    result_df = pd.DataFrame(CarSharingResult, columns = columns)
    result_df.to_csv(os.path.join(result_path, 'car_sharing_df.csv'), index=False, encoding='cp949')
    
    print(" >> 데이터 저장 완료")
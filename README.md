# 국내 렌터카 사업 분석 및 FAQ 조회 서비스
> **SK네트웍스 Family AI 캠프 2기** <br/> 
> **프로젝트 기간: 2024.06.03 ~ 2024.06.04** 
>
## 팀 소개
| 김영현 | 박주희 | 서종호 | 전상욱 | 전유빈 |

## 프로젝트 개요 및 소개
최근 이용이 증가하고 있는 Car sharing 서비스 업계 1,2위인 쏘카, 그린카와의 데이터를 비교 분석하여 증가하고 있는 새로운 공유 시장과 기존의 전통적인 렌트카 회사와의 차이점을 통해 SK렌트카 매각에 대한 원인과 Car sharing 서비스가 선택받는 이유를 알아보고자 함

## 수집 데이터
1. 전국 렌터카 업체 정보 표준데이터([link](https://www.data.go.kr/data/15025689/standard.do)): 지역별로 운영 중인 렌터카 업체에 대한 정보
2. 국토교통부 카셰어링 정보([link](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15098557)): 카셰어링 차고지의 이름, 주소, 좌표를 기반으로 차고지 정보(차고지명, 차고지ID, 주소, 좌표)를 조회하는 서비스 (*제공지역 : 쏘카(서울), 그린카(전국))
3. SK렌터카 FAQ ([link](https://homepage.skcarrental.com/customer/faq))

## 기능 소개
1. 렌트카 업체별 자동차 총 보유 대수 비교
    - 4사 (SK, 쏘카, 그린카, 롯데렌탈) + 기타
2. gps를 이용한 주요 렌터카 업체 및 보유 수 비교
    - SK렌터카, 쏘카, 그린카
3. FAQ 시스템

## 설치
``` bash
$ git clone https://github.com/SKNETWORKS-FAMILY-AICAMP/SKN02-1st-3Team.git
$ pip install -r requirements.txt
```

## 실행 가이드
1. `crawling_runner.py`: 전국 렌트카 정보 및 주요 렌트카 카셰어링 정보, sk렌트카 FAQ 등의 정보를 크롤링 및 전처리합니다.
```bash
$ python crawling_runner.py
```

1. `db_runner.py`: 크롤링한 데이터를 데이터베이스에 적재합니다.
```bash
$ python db_runner.py
```

1. `runner.py`: Streamlit을 이용하여 웹앱을 작동시킵니다.
```bash
$ streamlit run runner.py
```

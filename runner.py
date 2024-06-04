import folium
import streamlit as st
from streamlit_folium import st_folium
from folium import Marker
import pymysql
import plotly.express as px
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import csv

## db 연결
conn_skrentcardb = pymysql.connect(
    host = '127.0.0.1',
    user = 'root',
    password = '1234',
    db = 'skrentcardb',
    charset = 'utf8'
)

conn_total_rentcar_db = pymysql.connect(
    host = '127.0.0.1',
    user = 'root',
    password = '1234',
    db = 'total_rentcar_db',
    charset = 'utf8'
)

conn_socar_greencar_tbl = pymysql.connect(
    host = '127.0.0.1',
    user = 'root',
    password = '1234',
    db = 'socar_greencar_tbl',
    charset = 'utf8'
)

conn_sk_rentcar_faq_tbl = pymysql.connect(
    host = '127.0.0.1',
    user = 'root',
    password = '1234',
    db = 'sk_rentcar_faq',
    charset = 'utf8'
)




## 제목 
st.title('국내 렌터카 사업 분석 및 FAQ 조회 서비스')
 
# 탭 4개 생성
tab_titles = ['렌터카 업체 차량 보유 대수 비교', '렌터카 대여장소 지도 비교','SK렌터카 FAQ']
tab2, tab3, tab4 = st.tabs(tab_titles)

 
# 각 탭에 콘텐츠 추가
with tab2:
    st.header('렌터카 업체 차량 보유 대수 비교')
    st.write('SK렌터카 > 롯데 > 쏘카,그린카')

    curs = conn_total_rentcar_db.cursor()
    sql = "SELECT CASE WHEN name IN ('SK', '롯데렌탈', '쏘카', '그린카') THEN name ELSE 'etc' END AS company_category, SUM(car_count) AS total_car_count FROM total_rentcar_tbl GROUP BY company_category ORDER BY total_car_count DESC;"
    curs.execute(sql)
    total_rentcar_Result = curs.fetchall()


    

    # 색상 목록 만들기
    colors = ['gray', 'orange', 'red', 'green', 'blue']

    # 데이터 및 라벨 설정
    labels = ['ETC','SK','lotte','greencar&socar',' ']
    sizes = []

    # sizes 데이터 설정 (sizes가 무엇인지는 제공되지 않았지만, total_rentcar_Result와 동일하게 사용)
    for i in range(5):
        sizes.append(total_rentcar_Result[i][1])

    # 파이차트 그리기
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors) # colors 매개변수 추가
    ax.axis('equal')

    # 스트림릿에 파이차트 표시
    st.pyplot(fig)

#startangle는 부채꼴이 그려지는 시작 각도를 설정합니다.
#디폴트는 0도 (양의 방향 x축)로 설정되어 있습니다.
#counterclock=False로 설정하면 시계 방향 순서로 부채꼴 영역이 표시됩니다.
        
# db 당겨와서 지도 띄우기 
with tab3:
    # 제목, 소제목
    st.header('대여지점수 비교')
    st.write('그룹사별 대여지점 비교')

    # 지도 비율 지정
    column1, column2 = st.columns([1, 1])
    # 첫번째 지도
    with column1:
        st.write('쏘카 / 그린카 대여지점')

        curs = conn_socar_greencar_tbl.cursor()
        sql = "select * from socar_greencar_tbl where car_type = '1';"
        curs.execute(sql)
        socar_Rent_marking_Result = curs.fetchall()

        curs = conn_socar_greencar_tbl.cursor()
        sql = "select * from socar_greencar_tbl where car_type = '2';"
        curs.execute(sql)
        green_Rent_marking_Result = curs.fetchall()

        a = folium.Map(location=[36.95, 128.25], zoom_start=6)

        
        for i in range(len(socar_Rent_marking_Result)):
            folium.Marker([socar_Rent_marking_Result[i][4],socar_Rent_marking_Result[i][5]],
                          popup=socar_Rent_marking_Result[i][2],
                          tooltip=socar_Rent_marking_Result[i][3],
                          icon=folium.Icon(color='blue')).add_to(a)
        
        for i in range(len(socar_Rent_marking_Result)):
            folium.Marker([green_Rent_marking_Result[i][4],green_Rent_marking_Result[i][5]],
                          popup=green_Rent_marking_Result[i][2],
                          tooltip=green_Rent_marking_Result[i][3],
                          icon=folium.Icon(color='green')).add_to(a)
            
        st_data_a = st_folium(a, key="map_a",)
            

    # 두번째 지도
    with column2:
        st.write('SK렌터카 대여지점')
        # 시작지점
        b = folium.Map(location=[35.95, 128.25], zoom_start=7)

        curs = conn_skrentcardb.cursor()
        sql = "SELECT * FROM locationTbl"
        curs.execute(sql)
        sk_Rent_marking_Result = curs.fetchall()
        # 시작지점 전체 마커 띄우기
        # 이름과 툴팁 색 지정 
        for i in range(len(sk_Rent_marking_Result)):
            folium.Marker([sk_Rent_marking_Result[i][2],sk_Rent_marking_Result[i][3]],
                          popup=sk_Rent_marking_Result[i][1],
                          tooltip=sk_Rent_marking_Result[i][4],
                          icon=folium.Icon(color='red')).add_to(b)
            
        st_data_b = st_folium(b, key="map_b")

with tab4:
    st.header('FAQ')
    st.write('SK렌터카 FQA')

    # curs = conn_sk_rentcar_faq_tbl.cursor()
    # sql = "select * from sk_faq_tbl;"
    # curs.execute(sql)
    # conn_sk_rentcar_faq_tbl = curs.fetchall()

    # num_row = st.number_input("Number of Rows", min_value=1, max_value=4)
    # for i in range(7):
    #     with st.expander(conn_sk_rentcar_faq_tbl[i+1+(num_row*7)][0]):
    #         st.write(conn_sk_rentcar_faq_tbl[i+1+(num_row*7)][1])

    data = list()
    result_path = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_path, exist_ok=True)
    
    os.path.join(result_path, 'faq_df.csv')
    with open(os.path.join(result_path, 'faq_df.csv'),'r',encoding='UTF8') as f:
        rea = csv.reader(f)
        for row in rea:
            data.append(row)

    num_row = st.number_input("현재 페이지", min_value=1, max_value=4)
    for i in range(7):
        with st.expander(data[i+1+(num_row*7)][0]):
            st.write(data[i+1+(num_row*7)][1])


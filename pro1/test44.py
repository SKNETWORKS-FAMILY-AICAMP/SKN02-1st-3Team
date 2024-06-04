import folium
import streamlit as st
from streamlit_folium import st_folium
from folium import Marker
import pymysql

## db 연결
conn = pymysql.connect(
    host = '127.0.0.1',
    user = 'root',
    password = '1234',
    db = 'skrentcardb',
    charset = 'utf8'
)


## 제목 
st.title('sk렌터카 사업실패 꺌꺌')
 
# 탭 4개 생성
tab_titles = ['지역별 렌트카 수', '렌터카 업체별 사업장 및 차량 보유 대수 비교', '렌터카 대여장소 지도 비교','그린카 FAQ']
tab1, tab2, tab3, tab4 = st.tabs(tab_titles)

 
# 각 탭에 콘텐츠 추가
with tab1:
    st.header('주제 A')
    st.write('주제 A의 내용')

with tab2:
    st.header('FQA')
    st.write('SK렌터카 FQA')

        
# db 당겨와서 지도 띄우기 
with tab3:
    # 제목, 소제목
    st.header('대여지점수 비교')
    st.write('그룹사별 대여지점 비교')

    # 지도 비율 지정
    column1, column2 = st.columns([1, 1])
    # 첫번째 지도
    with column1:

        curs = conn.cursor()
        sql = "SELECT * FROM locationTbl"
        curs.execute(sql)
        sk_Rent_marking_Result = curs.fetchall()

        st.write('쏘카 / 그린카 대여지점')
        a = folium.Map(location=[36.95, 128.25], zoom_start=6)
        folium.Marker([37.4659942, 126.8895083], 
                      popup="Liberty Bell1", 
                      tooltip="Liberty Bell1",
                      ).add_to(a)
        folium.Marker([37.5659942, 126.8895083], 
                      popup="Liberty Bell2", 
                      tooltip="Liberty Bell2",
                      ).add_to(a)
        folium.Marker([37.6659942, 126.8895083], 
                      popup="Liberty Bell3", 
                      tooltip="Liberty Bell3",
                      ).add_to(a)
        st_data_a = st_folium(a, key="map_a",)
    # 두번째 지도
    with column2:
        st.write('SK렌터카 대여지점')
        # 시작지점
        b = folium.Map(location=[35.95, 128.25], zoom_start=7)
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


    import csv
    data = list()
    with open('C:/ex/pro1/db/faq_df.csv','r',encoding='UTF8') as f:
        rea = csv.reader(f)
        for row in rea:
            data.append(row)

    num_row = st.number_input("Number of Rows", min_value=1, max_value=4)
    for i in range(7):
        with st.expander(data[i+1+(num_row*7)][0]):
            st.write(data[i+1+(num_row*7)][1])


import folium
import streamlit as st

from streamlit_folium import st_folium

import pymysql

conn = pymysql.connect(
    host = '127.0.0.1'
    user = 'root'
    password = '1234'
    db = 'skrentcardb'
    charset = 'utf8'
)

curs = conn.cursor()
sql = "SELECT id, 업체명, 위도, 경도 FROM locationTbl"
curs.execute(sql)
sk_Rent_marking_Result = curs.fetchall()

# center on Liberty Bell, add marker

m = folium.Map(location=[37.4659942, 126.8895083], zoom_start=8)
folium.Marker(
    [37.4659942, 126.8895083], popup="Liberty Bell", tooltip="Liberty Bell"
).add_to(m)

# call to render Folium map in Streamlit
st_data = st_folium(m, width=725)
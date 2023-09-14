import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from snowflake.snowpark import functions as F
import folium
import streamlit as st
from streamlit_folium import  folium_static
import matplotlib.pyplot as plt
import geopandas as gpd
import streamlit.components.v1 as components
import contextily as cx
from datetime import datetime, timedelta
from uuid import uuid4
from jwt import encode
import os,json
from PIL import Image
import plotly.express as px

###Set the streamlit page layout
st.set_page_config(layout="wide")

###Create Connection to Snowflake
@st.cache_resource(ttl=3600)
def get_db_session():

    CONNECTION_PARAMETERS = {
    'url': st.secrets["account"],
    'ACCOUNT': st.secrets["account"],
    'user': st.secrets["username"],
    'password': st.secrets["password"],
    'database': st.secrets["database"],
    'warehouse': 'STREAMLIT',
    'role': st.secrets["role"],
    'schema': 'RAW'
    }
    return Session.builder.configs(CONNECTION_PARAMETERS).create()

session = get_db_session()



st.title("UK OPEN ROADS PROVIDED BY OS")


def rclass():
    df = session.table('ROAD_NETWORK').select('"class"').distinct().to_pandas()
    return df

def road(rclass):
    df = session.table('ROAD_NETWORK').filter(F.col('"class"') ==rclass).select('"roadNumber"').distinct().to_pandas()
    return df

def road_junction():
    return session.table('ROAD_JUNCTIONS').with_column('WKT',F.call_function('ST_ASWKT',F.col('GEOGRAPHY')))
    




#df.filter(F.col('"Modified Date"') == max_mod_date)
with st.sidebar:
    
    st.image( Image.open('pages/os_logo.png'))
    rclass = st.selectbox('select class: ',rclass(),index = 7)
    road = st.selectbox('select road: ',road(rclass))
    
        
st.markdown(f"#### Road Details for {rclass} {road}")
st.markdown(f"##### Hover over each road segment for details")

    
junctions = road_junction()


data = session.table('ROAD_NETWORK').filter((F.col('"class"') == rclass)&(F.col('"roadNumber"')==road))\
    .select('"formOfWay"','"length"','"shapefile"','"name1"','"identifier"')\
    .with_column('"centroid"',F.call_function('ST_CENTROID',F.col('"shapefile"')))\
    .with_column('long',F.call_function('ST_X',F.col('"centroid"')))\
    .with_column('lat',F.call_function('ST_Y',F.col('"centroid"')))\
    .with_column('WKT',F.call_function('ST_ASWKT',F.col('"shapefile"')))\
    .with_column('GEOMETRY',F.call_function('TO_GEOMETRY',F.col('WKT')))\
    .with_column('BUFFER',F.call_function('ST_BUFFER',F.col('GEOMETRY'),0.01))\
    .with_column('WKT_BUFFER',F.call_function('ST_ASWKT',F.col('BUFFER')))\
    #.join(session.table('ROAD_JUNCTIONS'))#\
    #.drop('"shapefile"').drop('"centroid"','BUFFER','GEOMETRY')
junctions_joined = junctions.join(data.select('"identifier"','"shapefile"'),
                                  F.call_function('ST_DWITHIN',F.col('GEOGRAPHY'),F.col('"shapefile"'),50),type='left')\
                                  .with_column('LAT',F.call_function('ST_Y',F.col('GEOGRAPHY')))\
                                  .with_column('LON',F.call_function('ST_X',F.col('GEOGRAPHY')))

setlong = data.agg(F.avg('LAT').alias('LAT'),F.avg('LONG').alias('LONG')).to_pandas()
LAT = setlong.LAT.iloc[0]
LONG = setlong.LONG.iloc[0]

##st.write(junctions_joined)

#st.table(data.group_by('"formOfWay"','"name1"').agg(F.sum('"length"').alias("total length")).limit(4))

#display on a map
geom = gpd.GeoDataFrame(data.to_pandas())
geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
geodframe.crs = "EPSG:4326"
geojson = geodframe.to_json(drop_id=True)

geodframe2 = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT_BUFFER']))
geodframe2.crs = "EPSG:4326"
geojson2 = geodframe2.to_json(drop_id=True)
#st.write(geojson)

geom2 = junctions_joined.to_pandas()


m2 = folium.Map(location=[LAT,LONG], zoom_start=9, tiles="openstreetmap")





cp2 = folium.Choropleth(
                geo_data = geojson2,
                name = 'Buffer',
                data = geodframe2,
                columns = ['identifier','length'],
                key_on = 'properties.identifier',
                fill_color = 'Blues',
                fill_opacity = 0.6,
                line_opacity = 0,
                legend_name = 'Length of Stretch',
                smooth_factor = 0,
                highlight = True
                ).add_to(m2)

cp2.geojson.add_child(
                folium.features.GeoJsonTooltip(['formOfWay',
                                                'name1',
                                    'length',
                                    'LONG',
                                    'LAT'
                                    ]))

cp1 = folium.Choropleth(
                geo_data = geojson,
                name = 'Road',
                data = geodframe,
                columns = ['identifier','length'],
                #key_on = 'properties.identifier',
                #fill_color = 'Greens',
                #fill_opacity = 0,
                line_opacity = 1,
                legend_name = 'Length of Stretch',
                smooth_factor = 0,
                highlight = True
                ).add_to(m2)

cp1.geojson.add_child(
                folium.features.GeoJsonTooltip(['formOfWay',
                                    'length',
                                    'LONG',
                                    'LAT'
                                    ]))

for A in range (0,geom2.shape[0]):

    html = f'''<body style="background-color:#F0F0F0;">
    <p style="font-family:verdana"> 
    Junction Number: {geom2.JUNCTION_NUMBER.iloc[A]} '''
    iframe = folium.IFrame(html,width=300,height=300)
    popup = folium.Popup(iframe,max_width=300)
    folium.Marker(geom2[['LAT', 'LON']].iloc[A].to_numpy(),popup = popup, icon=folium.Icon(color='blue', prefix='fa',icon='car'), icon_size=(40,40)).add_to(m2)

folium.LayerControl().add_to(m2)
folium_static(m2, width=1200, height= 700)
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



@st.cache_data
def accident_year_array():
    accident_year_array = session.table('ACCIDENTS')\
        .filter(F.col("LATITUDE").isNotNull())\
            .select('ACCIDENT_YEAR')\
                .group_by(F.col('ACCIDENT_YEAR')\
                    .cast(StringType()))\
                        .agg(F.min('ACCIDENT_YEAR').alias('YEAR'))\
                            .sort(F.col('YEAR').desc())\
                                .select(F.col('''CAST(ACCIDENT_YEAR)''').alias('YEAR'))\
                                    .to_pandas().YEAR
    return accident_year_array

st.title("LEVERAGING UK OPEN ROAD DATA AS PROVIDED BY OS")


def rclass():
    df = session.table('ROAD_NETWORK').select('"class"').distinct().to_pandas()
    return df

def road(rclass):
    df = session.table('ROAD_NETWORK').filter(F.col('"class"') ==rclass).select('"roadNumber"').distinct().to_pandas()
    return df

def road_junction():
    return session.table('ROAD_JUNCTIONS').with_column('WKT',F.call_function('ST_ASWKT',F.col('GEOGRAPHY')))\
                                                       .with_column('LAT',F.call_function('ST_Y',F.col('GEOGRAPHY')))\
                                                    .with_column('LON',F.call_function('ST_X',F.col('GEOGRAPHY')))
    




#df.filter(F.col('"Modified Date"') == max_mod_date)
with st.sidebar:
    
    st.image( Image.open('pages/os_logo.png'))
    select_junction = st.selectbox('Junction',road_junction().select('JUNCTION_NUMBER').sort('JUNCTION_NUMBER').to_pandas())
    distance = st.slider(f'Distance in Metres',1,5000,500)
    S_YEAR = st.selectbox("Year for Vehicle Incidents",(accident_year_array()))
    res = st.number_input('Choose Resolution between 7 and 12',7,12,9)
    on = st.toggle('Show Road Nodes')
        


    
junctions = road_junction()

selected_junction_data = road_junction().filter(F.col('JUNCTION_NUMBER')==select_junction)

st.markdown(f"#### Road network with related traffic incidents during {S_YEAR} within a {distance}M radius of junction {select_junction}")



data = session.table('ROAD_NETWORK')\
    .select('"formOfWay"','"length"','"shapefile"','"name1"','"identifier"','"class"')\
    .with_column('"centroid"',F.call_function('ST_CENTROID',F.col('"shapefile"')))\
    .with_column('long',F.call_function('ST_X',F.col('"centroid"')))\
    .with_column('lat',F.call_function('ST_Y',F.col('"centroid"')))\
    .with_column('WKT',F.call_function('ST_ASWKT',F.col('"shapefile"')))\
    .with_column('GEOMETRY',F.call_function('TO_GEOMETRY',F.col('WKT')))\
    .with_column('GEOMTYPE',F.call_function('ST_ASGEOJSON',F.col('"shapefile"'))['type'])
junctions_joined = junctions.join(data.select('"identifier"','"shapefile"','WKT','"name1"'
                                              ,'"length"','"formOfWay"','"class"',F.col('GEOMTYPE').cast(StringType()).alias('GEOMTYPE')),
                                  F.call_function('ST_DWITHIN',F.col('GEOGRAPHY'),F.col('"shapefile"'),distance),type='inner',lsuffix='L')\
                                  .filter(F.col('JUNCTION_NUMBER')==select_junction)
                                  
@st.cache_data
def setlong(junction):
    return junctions_joined.agg(F.avg('LAT').alias('LAT'),F.avg('LON').alias('LON')).to_pandas()

setlong = setlong(select_junction)
LAT = setlong.LAT.iloc[0]
LONG = setlong.LON.iloc[0]

@st.cache_data
def geom(year,junction,radius):
    return gpd.GeoDataFrame(junctions_joined.filter(F.col('GEOMTYPE')!='Point')\
                        .select('"identifier"','WKT','"name1"','"length"','"formOfWay"','"class"','GEOMTYPE')\
                        .to_pandas())

geom = geom(S_YEAR,select_junction,distance)



sqlcp2 = f'''SELECT

            LONGITUDE,
            LATITUDE, 
            ST_MAKEPOINT(LONGITUDE,LATITUDE) POINT,
            ANALYTICS_TOOLBOX.CARTO.H3_FROMLONGLAT(LONGITUDE,LATITUDE,{res}) H3INDEX,
            1 TOTAL_INCIDENTS, 
            NUMBER_OF_CASUALTIES TOTAL_CASUALTIES,
            NUMBER_OF_VEHICLES TOTAL_VEHICLES 
            FROM ACCIDENTS
            WHERE ACCIDENT_YEAR = '{S_YEAR}'
                        '''


incident_points = session.sql(sqlcp2)

@st.cache_data
def geomp(year,junction,radius):
    return gpd.GeoDataFrame(junctions_joined.filter(F.col('GEOMTYPE')=='Point').select('"identifier"','WKT','"shapefile"','GEOMTYPE')\
    .with_column('long',F.call_function('ST_X',F.col('"shapefile"')))\
    .with_column('lat',F.call_function('ST_Y',F.col('"shapefile"')))\
    .drop('"shapefile"')
                         
                         .to_pandas())


geomp = geomp(S_YEAR,select_junction,distance)


@st.cache_data
def filter_incidents2(year,junction,radius,res):
    return junctions.join(incident_points,
                    F.call_function('ST_DWITHIN',F.col('GEOGRAPHY'),F.col('POINT'),distance),type='inner',lsuffix='L')\
                    .filter(F.col('JUNCTION_NUMBER')==select_junction).drop('POINT')\
                    .group_by('H3INDEX')\
                        .agg(F.sum('TOTAL_CASUALTIES').alias('TOTAL_CASUALTIES'),
                             F.sum('TOTAL_INCIDENTS').alias('TOTAL_INCIDENTS'),
                             F.sum('TOTAL_VEHICLES').alias('TOTAL_VEHICLES'))\
                             .with_column('WKT',
                                          F.call_function('ST_ASWKT',F.call_function('ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY',F.col('H3INDEX')))).to_pandas()


geodframe = geom.set_geometry(gpd.GeoSeries.from_wkt(geom['WKT']))
geodframep = geomp.set_geometry(gpd.GeoSeries.from_wkt(geomp['WKT']))
filt_incidents = filter_incidents2(S_YEAR,select_junction,distance,res)

geodframei = filt_incidents.set_geometry(gpd.GeoSeries.from_wkt(filt_incidents['WKT']))

geodframe.crs = "EPSG:4326"
geodframep.crs = "EPSG:4326"
geodframei.crs = "EPSG:4326"

geojson = geodframe.to_json(drop_id=True)
geojsonp = geodframep.to_json(drop_id=True)
geojsoni = geodframei.to_json(drop_id=True)

geom2 = junctions_joined.to_pandas()


m2 = folium.Map(location=[LAT,LONG], zoom_start=15, tiles="cartodbpositron")


#marker_cluster = folium.MarkerCluster().add_to(m2)

cp1 = folium.Choropleth(
                geo_data = geojson,
                name = 'Road',
                data = geodframe,
                columns = ['identifier','length'],
                #key_on = 'properties.identifier',
                #fill_color = 'Greens',
                #fill_opacity = 0,
                line_opacity = 1,
                line_color='black',
                smooth_factor = 0,
                highlight = True
                ).add_to(m2)

cp1.geojson.add_child(
                folium.features.GeoJsonTooltip(['formOfWay',
                                    'length',
                                    'name1',
                                    'class',
                                    'GEOMTYPE'
                                    
                                    ]))

cp2 = folium.Choropleth(
                geo_data = geojsoni,
                name = 'Incidents',
                data = geodframei,
                columns = ['H3INDEX','TOTAL_CASUALTIES'],
                key_on = 'properties.H3INDEX',
                fill_color = 'RdYlBu_r',
                fill_opacity = 0.5,
                line_opacity = 0.5,
                line_color='black',
                smooth_factor = 0,
                highlight = True
                ).add_to(m2)

cp2.geojson.add_child(
                folium.features.GeoJsonTooltip(['TOTAL_INCIDENTS',
                                    'TOTAL_CASUALTIES',
                                    'TOTAL_VEHICLES',
                                    'H3INDEX'
                                
                                    
                                    ]))



junctionspd = selected_junction_data.to_pandas()




if on:

    for A in range (0,geomp.shape[0]):
        html2 = f'''<body style="background-color:#F0F0F0;">
        <p style="font-family:verdana"> 
        Latitude: {geomp.LAT.iloc[A]} 
        Longitude: {geomp.LONG.iloc[A]} 
        Geo Type: {geomp.GEOMTYPE.iloc[A]} '''
        iframe2 = folium.IFrame(html2,width=300,height=300)
        popup2 = folium.Popup(iframe2,max_width=300)
        folium.Marker(geomp[['LAT', 'LONG']].iloc[A].to_numpy(),popup = popup2, icon=folium.Icon(color='blue',icon='info-sign'),  icon_size=(10,10)).add_to(m2)

html = f'''<body style="background-color:#F0F0F0;">
<p style="font-family:verdana"> 
Junction Number: {junctionspd.JUNCTION_NUMBER.iloc[0]} '''
iframe = folium.IFrame(html,width=300,height=300)
popup = folium.Popup(iframe,max_width=300)
M1 = folium.Marker(junctionspd[['LAT', 'LON']].iloc[0].to_numpy(),name = 'section',popup = popup, icon=folium.Icon(color='red', prefix='fa',icon='car'), icon_size=(40,40)).add_to(m2)


folium.LayerControl().add_to(m2)

try:
    folium_static(m2, width=1200, height= 800)

except:
    'no data available within chosen selection'
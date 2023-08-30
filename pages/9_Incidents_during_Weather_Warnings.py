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

def traffic_counts_with_warnings():
    df2 = session.table('TRAFFIC_COUNTS_WEATHER_WARNING_IMPACT')
    df2 = df2.with_column('BOUNDARY',F.call_function('ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY',F.col('H3INDEX')))
    return df2

@st.cache_data
def traffic_counts_with_warnings_pd():
    df3 = traffic_counts_with_warnings.to_pandas()
    return df3

@st.cache_data
def distinct_count_dates_with_warnings():
    dates = traffic_counts_with_warnings().select('COUNT_DATE').distinct().sort('COUNT_DATE',ascending=False).to_pandas()
    return dates

st.title("TRAFFIC COUNTS WITH WEATHER WARNINGS AND RELATED INCIDENTS")
st.markdown("Select Warning Headline followed by the modified date of the weather warning.  Compare Warnings with Vehicle Incidents by selecting the vehicle type.  In addition, compare Traffic Counts for a vehicle type.  The heatmap will reflect the vehicle types selected.")

def weather_warnings_on_date(input_1):
    df = session.table('"Weather Warnings Indexed"').filter((F.col('"Valid From Date"') <=input_1)& (F.col('"Valid To Date"') >=input_1))
    df = df.with_column('CENTROID',F.call_function('ST_CENTROID',F.col('"Geography"')))
    df = df.with_column('WKT',F.call_function('ST_ASWKT',F.col('H3BOUNDARY')))
    df = df.with_column('LONG',F.call_function('ST_X',F.col('CENTROID')))
    df = df.with_column('LAT',F.call_function('ST_Y',F.col('CENTROID')))
    return df


def weather_warnings_on_date_warning(input_1,input_2):
    df = weather_warnings_on_date(input_1)
    df = df.filter(F.col('"Warning Headline"')==input_2)
    return df

@st.cache_data
def weather_warnings_on_date_with_modified(input_1,input_2,moddate):
    df = weather_warnings_on_date_warning(input_1,input_2).filter(F.col('"Modified Date"') == str(moddate)).to_pandas()
    return df

@st.cache_data
def weather_warnings_on_datepd(input_1,input_2,moddate):
    df = weather_warnings_on_date_warning(input_1,input_2).to_pandas()
    df = df.loc[(df['Modified Date'] == moddate)]
    return df

def traffic_count_veh_types():
    df = session.table('"TRAFFIC_COUNTS_WEATHER_WARNING_IMPACT"').drop('DOW','MONTH','COUNT_DATE','H3INDEX','"Warning Impact"','"Warning Level"','"Weather Type"','BOUNDARY','WKT')
    df = df.columns
    return df

@st.cache_data
def weather_warnings_with_traffic_count(input_1):
    df2 = session.table('"TRAFFIC_COUNTS_WEATHER_WARNING_IMPACT"')
    df2 = df2.with_column('BOUNDARY',F.call_function('ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY',F.col('H3INDEX')))
    df2 = df2.with_column('WKT',F.call_function('ST_ASWKT',F.col('BOUNDARY')))
    df2 = df2.filter(F.col('COUNT_DATE')==input_1).to_pandas()
    return df2



def vehicle_incidents(input_1):
    return session.table('VEHICLE_INCIDENTS_VEHICLE_TYPE_H3_4').filter(F.col('DATE')==input_1).join(weather_warnings_on_date(input_1),lsuffix='l')\
    .group_by('DATE','VEHICLE_TYPE','H3INDEX','"Max Severity Score"')\
        .agg(F.sum('NUMBER_OF_VEHICLES').alias('NUMBER_OF_VEHICLES'))

def vehicle_incidents_type(input_1):
    return session.table('VEHICLE_INCIDENTS_VEHICLE_TYPE_H3_4').filter(F.col('DATE')==input_1)\
    .select('VEHICLE_TYPE').distinct()




@st.cache_data
def mod_date_list(input_1,input_2):
    return weather_warnings_on_date_warning(input_1,input_2).select('"Modified Date"').distinct().to_pandas()['Modified Date']


#df.filter(F.col('"Modified Date"') == max_mod_date)
with st.sidebar:
    st.caption('weather warning data provided by:')
    st.image( Image.open('pages/Met_Office.png'))
    


with st.form("my form"):


    input_1 = st.selectbox('Weather Warning Date.  The date is only available if there are matching traffic counts',distinct_count_dates_with_warnings())
    input_2 = st.selectbox('Warning Headline',weather_warnings_on_date(input_1).select('"Warning Headline"').distinct())


    col4, col5, col6 = st.columns(3)

    with col4:
            moddate  = st.selectbox('Modified Date',mod_date_list(input_1,input_2))
    
    
    with col5:
        vehicle_type = vehicle_incidents_type(input_1)
        
    
        vehtype =  st.selectbox ('Vehicle Type for incidents',vehicle_type)
  

    with col6:
        tcountm = st.selectbox('Traffic Count Measure',traffic_count_veh_types())

    submitted = st.form_submit_button("view results")
    
    if submitted:



        vehicle_incidents = vehicle_incidents(input_1).drop('DATE')
        vehicle_incidents = vehicle_incidents.filter(F.col('VEHICLE_TYPE') == vehtype)
        vehicle_incidents = vehicle_incidents.with_column('BOUNDARY',F.call_function('ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY',F.col('H3INDEX')))
        vehicle_incidents = vehicle_incidents.with_column('WKT',F.call_function('ST_ASWKT',F.col('BOUNDARY')))



        data = weather_warnings_on_datepd(input_1,input_2,moddate)

        data2 = weather_warnings_with_traffic_count(input_1)

        m2 = folium.Map(location=[data.LAT.iloc[0],data.LONG.iloc[0]], zoom_start=7, tiles="openstreetmap")

        #weather warnings layer        
        geodframe = gpd.GeoDataFrame(data)
        geodframe = geodframe.set_geometry(gpd.GeoSeries.from_wkt(geodframe['WKT'])) 
        geojson = gpd.GeoSeries(geodframe.set_index('H3INDEX')['geometry']).to_json()

        #traffic counts data
        geodframe2 = gpd.GeoDataFrame(data2)
        geodframe2 = geodframe2.set_geometry(gpd.GeoSeries.from_wkt(geodframe2['WKT']))
        geodframe2.crs = "EPSG:4326"
        geodframe2 = geodframe2.drop(['COUNT_DATE'], axis=1)
        geojson2 = gpd.GeoSeries(geodframe2.set_index('H3INDEX')['geometry']).to_json()
        geojson3 = geodframe2.to_json(drop_id=True)

        #vehicle incidents data
        geodframe3 = gpd.GeoDataFrame(vehicle_incidents.drop('BOUNDARY').to_pandas())
        geodframe3 = geodframe3.set_geometry(gpd.GeoSeries.from_wkt(geodframe3['WKT']))
        geodframe3.crs = "EPSG:4326"
        geojson4 = geodframe3.to_json(drop_id=True)

        #st.write(geodframe3)


        try:
            cp3 = folium.Choropleth(
                geo_data = geojson,
                name = 'Weather Warning Area',
                data = geodframe,
                columns = ['H3INDEX','Warning Impact'],
                key_on = 'feature.id',
                fill_color = 'Reds',
                fill_opacity = 0.3,
                line_opacity = 0,
                legend_name = 'Weather Warnings',
                smooth_factor = 0,
                Highlight = True
                ).add_to(m2)
        except:
            'no data'

        try:
            cp1 = folium.Choropleth(
                geo_data = geojson3,
                name = 'Traffic_Counts',
                data = geodframe2,
                columns = ['H3INDEX',tcountm],
                key_on = 'properties.H3INDEX',
                fill_color = 'Greens',
                fill_opacity = 0.6,
                line_opacity = 1,
                legend_name = vehtype,
                smooth_factor = 0,
                highlight = True
                ).add_to(m2)

            cp1.geojson.add_child(
                folium.features.GeoJsonTooltip(['PEDAL_CYCLES',
                                    'AVG_PEDAL_CYCLES',
                                    'PEDAL_CYCLES_VAR',
                                    'TWO_WHEELED_MOTOR_VEHICLES',
                                    'CARS_AND_TAXIS',
                                    'BUSES_AND_COACHES',
                                    'LGVS',
                                    'HGVS',
                                    'ALL_MOTOR_VEHICLES'])
         )
        except:
            'no data'

        try:
            cp4 = folium.Choropleth(
        geo_data = geojson4,
        name = 'Vehicle Incidents',
        data = geodframe3,
        columns = ['H3INDEX','NUMBER_OF_VEHICLES'],
        key_on = 'properties.H3INDEX',
        fill_color = 'Greys',
        fill_opacity = 0.7,
        line_opacity = 0.7,
        legend_name = 'Pedal Cycles',
        smooth_factor = 0,
        highlight = True
        ).add_to(m2)

            cp4.geojson.add_child(
                folium.features.GeoJsonTooltip(['Max Severity Score',
                                    'VEHICLE_TYPE',
                                    'NUMBER_OF_VEHICLES'
                                    ])
            )
        except:
            'no data'




        folium.LayerControl().add_to(m2)

        

       

       
    
    

        
        warning_info  = weather_warnings_on_datepd(input_1,input_2,moddate)
        valid_from_date = warning_info['Valid From Date'].iloc[0]
        valid_to_date = warning_info['Valid To Date'].iloc[0]
        warning_level = warning_info['Warning Level'].iloc[0]
        warning_Impact = warning_info['Warning Impact'].iloc[0]
        What_to_Expect = warning_info['What to Expect'].iloc[0]
        warning_details = warning_info['Warning Details'].iloc[0]
        warning_headline = warning_info['Warning Headline'].iloc[0]
        st.markdown(f'''### {warning_headline.upper()}''')
        st.markdown(f'''Warning Details: {warning_details}''')
        st.markdown(f'''What to Expect: {What_to_Expect}''')
        st.markdown(f'''Warning Level: {warning_level}''')
        st.markdown(f'''Warning Impact: {warning_Impact}''')

        st.markdown('Choose the layers on the right of the map to display weather warning area, Vehicle Incidents within the weather warning area or Traffic Counts')
        folium_static(m2, width=700, height= 700)
       # st.write(data2)
        st.markdown('#### TRAFFIC COUNT COMPARISONS WHERE EACH MARKER REPRESENTS A H3 INDEX')
        st.markdown('''Any measure prefixed 'AVG' represents the average number of vehicles counted for that day irrespective of weather warning.  Without is the actual number of vehicles counted during the selected weather warning. ''' )
        col3, col4, col5 = st.columns(3)



        with col3:
            fig = px.scatter( data2,
            x = data2[tcountm],
            y = data2.AVG_PEDAL_CYCLES,
             
            hover_data = {'H3INDEX'}
            )
            fig.update_traces(mode="markers")
            st.markdown(f'###### {tcountm} VS Average Pedal Cycles')
            st.plotly_chart(fig,use_container_width=True)

        with col4:
            
            fig = px.scatter( data2,
            x = data2[tcountm],
            y = data2.AVG_BUSES_AND_COACHES,
             
            hover_data = {'H3INDEX'}
            )
            fig.update_traces(mode="markers")  
            st.markdown(f'###### {tcountm} VS Average Buses and Coaches')
            st.plotly_chart(fig,use_container_width=True)          

        with col5:
            fig = px.scatter( data2,
            x = data2[tcountm],
            y = data2.AVG_CARS_AND_TAXIS,
             
            hover_data = {'H3INDEX'}
            )
            fig.update_traces(mode="markers")
            
            
            st.markdown(f'###### {tcountm} VS Average Cars and Taxis')    
            st.plotly_chart(fig,use_container_width=True)
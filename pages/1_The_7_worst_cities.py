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




#bring in a database table from snowflake using a snowpark dataframe

@st.cache_data
def worst_cities():
    return session.table("UK_Worst_Cities_To_Drive").to_pandas()



####here i am caching some data into streamlit from snowflake by utilising snowpark dataframes


#there are some icons i wish to cache - in this case i will be utilising the snowflake icon
@st.cache_data(ttl=3600)
def images_icons():
    return session.sql('''SELECT  *, GET_PRESIGNED_URL(@ICONS,RELATIVE_PATH,172800) URL FROM directory(@ICONS) ORDER BY RELATIVE_PATH''').to_pandas()

#there are some pictures of vehicle locations for worst places to drive - used snowpark to locate the URLs in the database and then cached the results to memory
@st.cache_data(ttl=3600)
def images_journeys():
    return session.sql('''SELECT  *, GET_PRESIGNED_URL(@IMAGES,RELATIVE_PATH,172800) URL FROM directory(@IMAGES) ORDER BY RELATIVE_PATH''').to_pandas()


#retrieve rows of data which contain the apparant worst cities to drive!
@st.cache_data
def retrieve_worst_cities():
    return session.table("UK_Worst_Cities_To_Drive").sort(F.col('RANK').asc()).to_pandas()


#this is for the accident year drop down list - to retrieve all relevant years of accidents where locations are recorded.  
# Utilising snowpark for python constructs


                
#a title for the streamlit app
st.title("VEHICLE INCIDENTS IN ENGLAND")


#Include a sidebar
with st.sidebar:
   


   
    #choose city selector in the sidebar - from the previously written snowflake function
   selected = st.radio("CHOOSE YOUR CITY:",retrieve_worst_cities().CITY)
  


st.markdown('##### Here is a map of all the worst locations to drive, as engineered in the notebook.  Click on the City tab for more insights.')
st.markdown('source - https://www.wessexfleet.co.uk/blog/2022/05/26/best-and-worst-uk-cities-for-driving/')
    
    #using pandas to filter the selections (this was initially retrieved via snowpark)
  
#create the folium map
selected_row = retrieve_worst_cities()[retrieve_worst_cities()["CITY"] == selected]
m = folium.Map(location=[selected_row.iloc[0].LATITUDE,selected_row.iloc[0].LONGITUDE], zoom_start=8, tiles="openstreetmap")

trail_coordinates = session.table("UK_Worst_Cities_To_Drive").sort('RANK').select('LATITUDE','LONGITUDE').to_pandas().to_numpy()

#add information to each point which includes tool tips

for A in range (0,7):
    
    html = f'''
    <body style="background-color:#F0F0F0;">
    <p style="font-family:verdana">
    <b> WORST DRIVING CITY INSIGHTS
    <BR><BR>
    <b>Rank: </b>{retrieve_worst_cities().RANK.iloc[A]}<BR><BR>
    <img src="{images_journeys().iloc[A].URL}", width=100>
    <br><br><b>City:</b>
    {retrieve_worst_cities().CITY.iloc[A]}<BR><BR>

    <hr>
        
    <p style="font-family:verdana">
        
    <b>STATS</b>
    <BR><BR>
    Crashes:  {retrieve_worst_cities()['CRASHES'].iloc[A]}
    <BR>
    Congestion Level: {retrieve_worst_cities()['Congestion Level %'].iloc[A]}
    <BR>
    Cars Per Parking Space: {retrieve_worst_cities()['Cars Per Parking Space'].iloc[A]}
    <BR>
    EV Charging Points: {retrieve_worst_cities()['EV Charging Points'].iloc[A]}
    <BR>
    Air Quality Index Score: {retrieve_worst_cities()['Air Quality Index Score'].iloc[A]}
    <BR>
    Bus Routes: {retrieve_worst_cities()['Bus Routes'].iloc[A]}
    <BR>
    Overall Score: {retrieve_worst_cities()['Overall Score'].iloc[A]}
    <BR>
    <hr>
    <p style="font-family:verdana">
    <b>Worst Junction: </b>{retrieve_worst_cities()['Bad Junction in City'].iloc[A]} 
    <BR><BR>
    <b>Source:</b><a href ="{retrieve_worst_cities().SOURCE.iloc[A]}" target="popup"> {retrieve_worst_cities().SOURCE.iloc[A]}</a></p>
    <hr>
    <p style="font-family:verdana">
    <br>
    Info Gathered from Accident Data
    <br>
    <BR>
    According to the data from the Department of Data, since the year 2000 
    there have been <font style="color:red"> {retrieve_worst_cities()['ACCIDENTS'].iloc[A]} </font> accidents.  
    Of which, <font style="color:red"> {retrieve_worst_cities()['VEHICLES'].iloc[A]} </font> vehicles and <font style="color:red"> {retrieve_worst_cities()['CASUALTIES'].iloc[A]} </font> 
    casualties were involved.

        
    '''

    html2 = f'''
    <body style="background-color:#F0F0F0;">
    <p style="font-family:verdana">
    <b> WORST DRIVING CITY INSIGHTS
    <BR><BR>
    <b>Rank: </b>{selected_row.iloc[0].RANK}<BR><BR>
    <img src="{images_journeys().iloc[selected_row.iloc[0].ID-1].URL}", width=100>
    <br><br><b>City:</b>
    {selected_row.iloc[0].CITY}<BR><BR>

    <hr>
        
    <p style="font-family:verdana">
        
    <b>STATS</b>
    <BR><BR>
    Crashes:  {selected_row.iloc[0].CRASHES}
    <BR>
    Congestion Level: {selected_row.iloc[0]['Congestion Level %']}
    <BR>
    Cars Per Parking Space: {selected_row.iloc[0]['Cars Per Parking Space']}
    <BR>
    EV Charging Points: {selected_row.iloc[0]['EV Charging Points']}
    <BR>
    Air Quality Index Score: {selected_row.iloc[0]['Air Quality Index Score']}
    <BR>
    Bus Routes: {selected_row.iloc[0]['Bus Routes']}
    <BR>
    Overall Score: {selected_row.iloc[0]['Overall Score']}
    <BR>
    <hr>
    <p style="font-family:verdana">
    <b>Worst Junction: </b>{selected_row['Bad Junction in City'].iloc[0]} 
    <BR><BR>
    <b>Source:</b><a href ="{selected_row.SOURCE.iloc[0]}" target="popup"> {selected_row.SOURCE.iloc[0]}</a></p>
    <hr>
    <p style="font-family:verdana">
    <br>
    Info Gathered from Accident Data
    <br>
    <BR>
    According to the data from the Department of Data, since the year 2000 
    there have been <font style="color:red"> {selected_row.ACCIDENTS.iloc[0]} </font> accidents.  
    Of which, <font style="color:red"> {selected_row.VEHICLES.iloc[0]} </font> vehicles and <font style="color:red"> {selected_row.CASUALTIES.iloc[0]} </font> 
    casualties were involved.

        
    '''
    iframe = folium.IFrame(html,width=700,height=400)
    iframe2 = folium.IFrame(html2,width=700,height=400)
    popup = folium.Popup(iframe,max_width=700)
    popup2 = folium.Popup(iframe2,max_width=700)
    folium.Marker(retrieve_worst_cities()[['LATITUDE', 'LONGITUDE']].iloc[A].to_numpy(),popup=popup, icon=folium.Icon(color='blue', prefix='fa',icon='car'), icon_size=(40,40)).add_to(m)
    folium.Marker(selected_row[['LATITUDE', 'LONGITUDE']].iloc[0],popup=popup2, icon=folium.Icon(color='red', prefix='fa',icon=f'{selected_row.RANK.iloc[0]}'), icon_size=(40, 40)).add_to(m)
    
    
    
st_data = folium_static(m, width=1200, height= 800)

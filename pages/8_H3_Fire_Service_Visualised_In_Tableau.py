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


#token generator for tableau automatic login

token = encode(
	{
		"iss": st.secrets["iss"],
		"exp": datetime.utcnow() + timedelta(minutes=5),
		"jti": str(uuid4()),
		"aud": "tableau",
		"sub": st.secrets["sub"],
		"scp": ["tableau:views:embed", "tableau:metrics:embed"],
		"Region": "East"
	},
		st.secrets["secret"],
		algorithm = "HS256",
		headers = {
		'kid': st.secrets["kid"],
		'iss': st.secrets["iss"]
        }
  )



 

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




                
#a title for the streamlit app
st.title("VEHICLE INCIDENTS NEAR FIRE SERVICES")

@st.cache_data
def fire_services():
    return session.sql('SELECT FRA22NM FROM FIRE_SERVICE_BOUNDARIES').to_pandas().FRA22NM
#Include a sidebar
with st.sidebar:
   

   
 
  
    #choose year selector to appear in the sidebar
   #S_YEAR = st.selectbox("Select Year",(accident_year_array()))
   with st.form("Modify View"):
    measure_array= ((2,3,4,5,6))
    list = [{'label':'All Casualties'},{'label':'Slight'},{'label':'Serious'},{'label':'Fatal'},{'label':'All Vehicles involved in Incidents'}]
    Text = pd.DataFrame(list).label
    measure = st.selectbox("Choose Measure",Text)
    FIRE_SERVICE = st.selectbox('Select  Fire Service',(fire_services()),3)
    submitted = st.form_submit_button("View Report")




st.markdown('#### VEHICLE INCIDENTS WITHIN A FIRE SERVICE AREA - VISUALISED IN TABLEAU')
st.markdown('###### Please select options from the side by and then click view report')

if submitted:   

    parameterno = pd.DataFrame(list).loc[pd.DataFrame(list)['label'] == measure].index[0]+2
    components.html(f'''<script type='module' 
src='https://eu-west-1a.online.tableau.com/javascripts/api/tableau.embedding.3.latest.min.js'>
</script><tableau-viz id='tableau-viz' src='https://10ax.online.tableau.com/t/beckysnowflake/views/carsFIRE1/FIRE_SERVICEintoH3' 
width='1300' height='1300' hide-tabs toolbar='hidden' token = {token} >
<viz-filter field="FRA22NM" value="{FIRE_SERVICE}"> </viz-filter>
<viz-parameter name="MEASURE" value={parameterno}>

        
</tableau-viz>''', height = 1300, width = 1300)


import streamlit as st
import streamlit.components.v1 as components
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
                
#a title for the streamlit app
st.title("INCIDENTS ACCROSS ALL MAIN TOWNS SHOWN IN TABLEAU")


#Include a sidebar
with st.sidebar:
   
   
    #place the snowflake logo which is stored in the database into the sidebar
   

   point_polygon = st.selectbox ("Choose Points or Polygons",('POINT','POLYGON'))

   

t1,t2 = st.columns(2)
with t1:
    
    components.html('''
    <p style="font-family:Verdana; color:#000000; font-size: 18px;">
    Colour Key:
    <p style="font-family:Verdana; color:#FF9F36; font-size: 18px;"> 
    Top 7 worst towns for driving 

    <p style="font-family:Verdana; color:#29B5E8; font-size: 18px;">All Other Towns''', height = 150) 
with t2:
    components.html('''
    <p style="font-family:Verdana; color:#000000; font-size: 18px;"> 
    Below, we are showing the number of accidents weighted by area of the town.  
    We utilised Snowflake's Geospatial features in order to create spatial 
    joins between the town geometries and the locations in which the accidents took place.
    
    ''')
    
   #here is the tableau embedded component - note the token variable - 
   # this is to automatically provide a log in to tableau without signing in.  
   # An app token has been created inside of tableau for this to work
   #point_polygon variable has been passed from streamlit to dynamically interact with the report
    
components.html(f'''<script type='module' 
    src='https://eu-west-1a.online.tableau.com/javascripts/api/tableau.embedding.3.latest.min.js'>
    </script>
    
    
    <tableau-viz id='tableau-viz' src='https://10ax.online.tableau.com/t/beckysnowflake/views/cars1/AccidentsByTown' 
    width='1600' height='1100' hide-tabs toolbar='hidden' token = {token}>
    
    <viz-parameter name="POINT/POLYGON" value={point_polygon}></viz-parameter>
    
    </tableau-viz>
    
    
    ''', height = 900, width = 1600)
   

 #here is another tableau report - again , token is passed to avoid logging in.  We are passing a year variable to interact
 #with the year parameter in tableau
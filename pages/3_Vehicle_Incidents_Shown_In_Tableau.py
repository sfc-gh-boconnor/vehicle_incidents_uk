import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from snowflake.snowpark import functions as F
import streamlit as st
from streamlit_folium import  folium_static
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



#this is for the accident year drop down list - to retrieve all relevant years of accidents where locations are recorded.  
# Utilising snowpark for python constructs

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
                
#a title for the streamlit app
st.title("VEHICLE INCIDENTS SHOWN IN TABLEAU")


#Include a sidebar
with st.sidebar:


   with st.form("Modify View"):
    S_YEAR = st.selectbox("Select Year",(accident_year_array()))
    submitted = st.form_submit_button("View Report")


st.markdown(f'##### VEHICLE INCIDENTS BROKEN DOWN BY ICB DURING {S_YEAR}')

if submitted:
    components.html(f'''<script type='module' 
src='https://eu-west-1a.online.tableau.com/javascripts/api/tableau.embedding.3.latest.min.js'>
</script><tableau-viz id='tableau-viz' 
src='https://10ax.online.tableau.com/t/beckysnowflake/views/cars1/AccidentsperMillionPopulation' 
width='1366' height='808' hide-tabs toolbar='hidden' token = {token}  >   
<viz-parameter name="Year" value={S_YEAR}>


</tableau-viz>''', height = 831, width = 1400)
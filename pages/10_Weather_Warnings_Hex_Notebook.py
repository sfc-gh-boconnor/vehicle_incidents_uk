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
from PIL import Image




###Set the streamlit page layout
st.set_page_config(layout="wide")

#df.filter(F.col('"Modified Date"') == max_mod_date)
with st.sidebar:
    st.caption('weather warning data provided by:')
    st.image( Image.open('pages/Met_Office.png'))
                
#a title for the streamlit app
st.title("Weather Warnings in a HEX notebook")




st.markdown(f'##### Weather Warnings with Vehicle counts in a Hex Notebook')


components.iframe('''https://app.hex.tech/snowflake/app/36ac67d8-ab69-4ff6-a472-e1fb2e8ba557/latest''', height = 1000, width = 1400)
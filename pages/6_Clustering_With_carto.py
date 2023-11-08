import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from snowflake.snowpark import functions as F
import streamlit as st
import matplotlib.pyplot as plt
import geopandas as gpd
import streamlit.components.v1 as components
import contextily as cx
from datetime import datetime, timedelta
from uuid import uuid4
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
@st.cache_data
def images_icons():
    return session.sql('''SELECT  *, GET_PRESIGNED_URL(@ICONS,RELATIVE_PATH,172800) URL FROM directory(@ICONS) ORDER BY RELATIVE_PATH''').to_pandas()

#there are some pictures of vehicle locations for worst places to drive - used snowpark to locate the URLs in the database and then cached the results to memory
@st.cache_data
def images_journeys():
    return session.sql('''SELECT  *, GET_PRESIGNED_URL(@LOCATIONS,RELATIVE_PATH,172800) URL FROM directory(@LOCATIONS) ORDER BY RELATIVE_PATH''').to_pandas()


#retrieve rows of data which contain the apparant worst cities to drive!
@st.cache_data
def retrieve_worst_cities():
    return session.table("UK_Worst_Cities_To_Drive").sort(F.col('RANK').asc()).to_pandas()


#caching the ICB geometries utilising snowpark data frame
@st.cache_data
def ICB():
    return session.table('ICB_GEOMETRIES').select('ICB22NM').to_pandas()



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
st.title("UTILIZING KMEANS CLUSTERING")
st.markdown("#### UTILISING CARTO FUNCTIONS PROVIDED ON THE SNOWFLAKE MARKETPLACE")


#Include a sidebar
with st.sidebar:
   
    #place the snowflake logo which is stored in the database into the sidebar
   

   
    #choose city selector in the sidebar - from the previously written snowflake function
   selected = st.radio("CHOOSE YOUR CITY:",retrieve_worst_cities().CITY)
  
    #choose year selector to appear in the sidebar
   S_YEAR = st.selectbox("Select Year",(accident_year_array()))

    #some text narrative ---stored in snowflake

   clusters = st.slider('Select  Clusters',1,10,7)
   
  
   
   
        
   #define the main tabs







#tab 1 - this is a map of the 7 worst places to drive


    
#this tab contains a H3 example giving the user tha bility to change the resolution.  in addition, the bottom half of this page 
#shows an example of clustering - both H3 and clustering are standard Carto functions which are instantly available in snowflake via
#the market place
df2 = retrieve_worst_cities()
df2 = df2.loc[(df2['CITY']==selected)]
S_TOWN_CODE = df2.TOWN_CODE.iloc[0]


    
ACCIDENTS = session.table('ACCIDENTS')

#caching the results shen selecting the resolution, year and town in order to display a heatmap plus several scatter plots


#caching the clustering results - utilising the KMEANS clustering algorythm
@st.cache_data(max_entries=4)
def clustering(C,S_YEAR, S_TOWN_CODE):

    sql = f'''
        
        select value:cluster cluster, st_aswkt( to_geometry( value:geom)) wkt  from

        (
        select 
        ANALYTICS_TOOLBOX.CARTO.ST_CLUSTERKMEANS(

        array_agg(
        st_asgeojson(st_makegeompoint(b.longitude,b.latitude))::string
        )
        ,{C}) clusters


        from 
        VEHICLE_ACCIDENT_DATA.RAW.ACCIDENTS_WITH_TOWN_CITY_DETAILS  a

        inner join accidents b on 

        a.accident_index = b.accident_index 
        and b.accident_year = {S_YEAR}
        and  a.town_code = '{S_TOWN_CODE}' 
        ), lateral flatten(clusters)
        
        '''

    return session.sql(sql).to_pandas()


    
st.markdown('''#### USING CARTO'S KMEANS CLUSTERING''')



clusters123 = clustering(clusters, S_YEAR, S_TOWN_CODE)
clusters123 = clusters123.groupby('CLUSTER').count().rename(columns={'S_YEAR': 'Incidents'})
st.bar_chart (clusters123)

    
    

        
geodframe = gpd.GeoDataFrame(clustering(clusters,S_YEAR, S_TOWN_CODE))
geodframe = geodframe.set_geometry(gpd.GeoSeries.from_wkt(geodframe['WKT'])) 
fig, ax = plt.subplots(1, figsize=(20, 10))
ax.axis('off')
geodframe.crs = "EPSG:4326"
geodframe.plot(column = geodframe.CLUSTER, cmap = 'viridis',alpha=0.7, legend=True, ax=ax, figsize=(9, 10))

        
cx.add_basemap(ax, crs=geodframe.crs, source=cx.providers.OpenStreetMap.Mapnik, zoom=12)
       

       
st.pyplot(fig)
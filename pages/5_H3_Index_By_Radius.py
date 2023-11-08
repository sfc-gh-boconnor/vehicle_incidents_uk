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



#token generator for tableau automatic login

 

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
st.title("H3 INDEXING AND VISUALISATION")
st.markdown("#### UTILISING CARTO FUNCTIONS PROVIDED ON THE SNOWFLAKE MARKETPLACE")


#Include a sidebar
with st.sidebar:
   
   
    #choose city selector in the sidebar - from the previously written snowflake function
   selected = st.radio("CHOOSE YOUR CITY:",retrieve_worst_cities().CITY)
  
    #choose year selector to appear in the sidebar
   S_YEAR = st.selectbox("Select Year",(accident_year_array()))

   
    

   values = st.slider('Select  H3 resolution',1,10,7,2)

   radius = st.slider('Select  Radius',1,10000,1000)

   measure = st.radio("Choose Measure:",['CASUALTIES','VEHICLES','ACCIDENTS','AVERAGE_AGE_OF_DRIVER','MALE_DRIVERS','FEMALE_DRIVERS'],2)




df2 = retrieve_worst_cities()
df2 = df2.loc[(df2['CITY']==selected)]
S_TOWN_CODE = df2.TOWN_CODE.iloc[0]

    
ACCIDENTS = session.table('ACCIDENTS')


    #this tab shows an example of h3 within a radius of a city's centre point.  You will see i am utilising the st_centroid
    #which gives you the centre of a polygon - standard snowflake function.


@st.cache_data(max_entries=4)
def RADIUS(RA, R, S_YEAR, S_TOWN_CODE):
        # Filter TOWNS_CITIES table based on TOWN_CODE and get the centroid
    ACCIDENT_TOWN_CITY = session.table('TOWNS_CITIES').filter(F.col('TOWN_CODE') == S_TOWN_CODE) \
            .select(F.call_udf('st_centroid', F.col('GEOMETRY')).alias('CENTROID')) \
            .select('CENTROID')

        # Join ACCIDENTS and ACCIDENT_TOWN_CITY tables based on ACCIDENT_YEAR
    ACCIDENT_DETAILS = ACCIDENTS.filter(F.col('ACCIDENT_YEAR') == S_YEAR) \
            .cross_join(ACCIDENT_TOWN_CITY) \
            .with_column('DISTANCE',
                        F.call_udf('st_distance', F.col('CENTROID'), F.call_udf('st_makepoint', F.col('LONGITUDE'),
                                                                               F.col('LATITUDE')))) \
            .filter(F.col('DISTANCE') < RA)

        # Select necessary columns and apply UDFs
    df = ACCIDENT_DETAILS.select('LATITUDE', 'LONGITUDE', 'ACCIDENT_INDEX', 'ACCIDENT_YEAR') \
            .with_column('H3', F.call_udf('ANALYTICS_TOOLBOX.CARTO.H3_FROMLONGLAT', F.col('LONGITUDE'),
                                      F.col('LATITUDE'), R)) \
            .with_column('H3_BOUNDARY', F.call_udf('ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY', F.col('H3'))) \
            .with_column('WKT', F.call_udf('ST_ASWKT', F.col('H3_BOUNDARY')))

        # Group by H3 and aggregate accidents
    accidents = df.group_by('H3').agg(F.any_value('WKT').alias('WKT'), F.count('ACCIDENT_INDEX').alias('ACCIDENTS'))

        # Join casualties table and calculate casualties
    casualties_tot = df.join(session.table('CASUALTIES').select('AGE_OF_CASUALTY', 'ACCIDENT_INDEX', 'CASUALTY_SEVERITY'),
                             'ACCIDENT_INDEX')

    casualties = df.join(session.table('CASUALTIES').select('AGE_OF_CASUALTY', 'ACCIDENT_INDEX', 'CASUALTY_SEVERITY'),
                         'ACCIDENT_INDEX') \
            .with_column('FATAL', F.when(F.col('CASUALTY_SEVERITY') == 1, F.lit(1))) \
            .with_column('SEVERE', F.when(F.col('CASUALTY_SEVERITY') == 2, F.lit(1))) \
            .with_column('MINOR', F.when(F.col('CASUALTY_SEVERITY') == 3, F.lit(1))) \
            .group_by('H3').agg(F.any_value('WKT').alias('WKT'), F.count('ACCIDENT_INDEX').alias('CASUALTIES'),
                                F.avg('AGE_OF_CASUALTY').alias('AVERAGE_AGE_OF_CASUALTY'),
                                F.sum('FATAL').alias('FATAL'), F.sum('SEVERE').alias('SEVERE'), F.sum('MINOR').alias('MINOR'))

        # Join vehicles table and calculate vehicles
    vehicles = df.join(
            session.table('VEHICLES').select(F.when(F.col('SEX_OF_DRIVER') == 1, F.lit(1)).alias('MALES'),
                                            F.when(F.col('SEX_OF_DRIVER') == 2, F.lit(1)).alias('FEMALES'),
                                            'ACCIDENT_INDEX', 'AGE_OF_DRIVER'), 'ACCIDENT_INDEX') \
            .group_by('H3') \
            .agg(F.any_value('WKT').alias('WKT'), F.count('ACCIDENT_INDEX').alias('VEHICLES'),
                F.avg('AGE_OF_DRIVER').alias('AVERAGE_AGE_OF_DRIVER'),
                F.sum('MALES').alias('MALE_DRIVERS'), F.sum('FEMALES').alias('FEMALE_DRIVERS'))

        # Join all the aggregated data
    all_data = accidents.join(casualties, 'H3', lsuffix="left") \
        .select('H3', F.col('WKT').alias('WKT2'), 'ACCIDENTS', 'CASUALTIES', 'FATAL', 'SEVERE', 'MINOR',
                    'AVERAGE_AGE_OF_CASUALTY').join(vehicles, 'H3', 'LEFT') \
            .select('H3', 'WKT', 'ACCIDENTS', 'CASUALTIES', 'VEHICLES', 'FATAL', 'SEVERE', 'MINOR',
                    'AVERAGE_AGE_OF_DRIVER', 'AVERAGE_AGE_OF_CASUALTY', 'MALE_DRIVERS', 'FEMALE_DRIVERS')

    return all_data.to_pandas()


        
    #st.write(RADIUS(radius,values,S_YEAR, S_TOWN_CODE))

st.markdown(f'''#### HEAT MAP OF INCIDENT DATA IN {selected.upper()}''' )
col20, col21 = st.columns(2)
    

with col20:
    st.write('Below I am selecting the measure used to shade in the H3 points of data on the map')
    
    selected_data = RADIUS(radius,values,S_YEAR, S_TOWN_CODE).sort_values(by=measure).head(1)

        

    if measure == 'AVERAGE_AGE_OF_DRIVER':
        TOTAL = RADIUS(radius,values,S_YEAR, S_TOWN_CODE)[measure].mean()
    else:
        TOTAL = RADIUS(radius,values,S_YEAR, S_TOWN_CODE)[measure].sum()
        
    st.markdown(f'I am utilising the Carto functionality to breakup the data for **{selected}** within **{S_YEAR}** into hexagons.  You will see that the total for **{measure}** is **{str(TOTAL)}**')
    

    
with col21:
        
    geodframe = gpd.GeoDataFrame(RADIUS(radius,values,S_YEAR, S_TOWN_CODE))
        #geodframe['geometry'] = gpd.GeoSeries.from_wkt(geodframe['WKT'])  
    geodframe = geodframe.set_geometry(gpd.GeoSeries.from_wkt(geodframe['WKT'])) 
    fig, ax = plt.subplots(1, figsize=(20, 10))
    ax.axis('off')
    geodframe.crs = "EPSG:4326"
    geodframe.plot(column = measure, cmap = 'Blues_r',alpha=0.7,ax=ax, figsize=(9, 10))

        
    cx.add_basemap(ax, crs=geodframe.crs, source=cx.providers.OpenStreetMap.Mapnik, zoom=12)# Create colorbar as a legend
    sm = plt.cm.ScalarMappable(cmap='Blues_r', norm=plt.Normalize(vmin=0, vmax=selected_data.iloc[0][measure]))
    sm._A = []
    cbar = fig.colorbar(sm, ax = ax)
    st.pyplot(fig)

    


col3, col4, col5 = st.columns(3)

    

with col3:
    fig, ax = plt.subplots()
    d = RADIUS(radius,values,S_YEAR, S_TOWN_CODE)
    x = d[measure]
    y = d.AVERAGE_AGE_OF_DRIVER
    ax.scatter(x,y) 
    ax.set_xlabel(measure)  
    ax.set_ylabel('AVERAGE_AGE_OF_DRIVER') 

    st.markdown(f'###### {measure} VS AVG AGE OF DRIVER')   
    st.pyplot(fig)

with col4:
    fig, ax = plt.subplots()
    d = RADIUS(radius,values,S_YEAR, S_TOWN_CODE)
    x = d[measure]
    y = d.AVERAGE_AGE_OF_CASUALTY
    ax.scatter(x,y) 
    ax.set_xlabel(measure)  
    ax.set_ylabel('AVERAGE AGE OF CASUALTY') 
    st.markdown(f'###### {measure} VS AVG AGE OF CASUALTY')    
    st.pyplot(fig)


with col5:
    st.markdown(f'###### {measure} VS FEMALE DRIVERS')
    fig, ax = plt.subplots()
    d = RADIUS(radius,values,S_YEAR, S_TOWN_CODE)
    x = d[measure]
    y = d.FEMALE_DRIVERS
    ax.scatter(x,y) 
    ax.set_xlabel(measure)  
    ax.set_ylabel('FEMALE DRIVERS')  
    st.pyplot(fig)

st.write(d)



#the tab here is actually utilising 3 sub tabs all about a chosen fire service

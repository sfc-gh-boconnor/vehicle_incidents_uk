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
st.title("VEHICLE INCIDENTS NEAR FIRE SERVICES")


#Include a sidebar
with st.sidebar:
   

  
    #choose year selector to appear in the sidebar
   S_YEAR = st.selectbox("Select Year",(accident_year_array()))

   @st.cache_data
   def fire_services():
        return session.sql('SELECT FRA22NM FROM FIRE_SERVICE_BOUNDARIES').to_pandas().FRA22NM
    
    
    
   FIRE_SERVICE = st.selectbox('Select  Fire Service',(fire_services()),3)


st.markdown('#### VEHICLE INCIDENTS WITHIN A FIRE SERVICE AREA')
    
#the 3 sub tabs
firetab1, firetab3 = st.tabs(["Incidents near Fire Stations","Predict Casualties near a Fire Station"])


    #this contains another folium map which shows firestations within a fire service and then behind these, a heatmap of vehicle incident
    #and casualty events.

with firetab1:
    ftcol1,ftcol2 = st.columns(2)
    with ftcol1:
        st.markdown('##### INCIDENTS NEAR FIRESTATIONS')
    with ftcol2:
        RF2 = st.slider('Select Distance Range in Metres within a local fire station',0, 10000, (0, 500))
        st.write( RF2)
        
       


    @st.cache_data(max_entries=4)
    def RADIUS_firestation_agg(S_YEAR,FIRE_SERVICE,RF2):

        data = session.table('VEHICLE_INCIDENT_VIEW')

        sqlcp = f'''SELECT H3INDEX, ANY_VALUE(ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY(H3INDEX)) GEOM, st_aswkt(GEOM) WKT, 
                        SUM(TOTAL_INCIDENTS) TOTAL_INCIDENTS, 
                        SUM(TOTAL_CASUALTIES) TOTAL_CASUALTIES 
                        FROM accidents_with_perc_fatal_with_locations
                        WHERE YEAR = '{S_YEAR}' AND FRA22NM = '{FIRE_SERVICE}'
                        GROUP BY H3INDEX'''
        chloropleth = session.sql(sqlcp)\
            .select(F.call_udf('st_centroid',F.col('GEOM'))\
                .alias('CENTROID'),'H3INDEX','GEOM','WKT','TOTAL_INCIDENTS','TOTAL_CASUALTIES')\
                .select('H3INDEX','CENTROID','GEOM','WKT','TOTAL_INCIDENTS','TOTAL_CASUALTIES',F.call_udf('ST_X',F.col('CENTROID'))\
                    .alias('LONCTR'),F.call_udf('ST_Y',F.col('CENTROID'))\
                        .alias('LATCTR'))

        firestations = session.table('FIRE_STATIONS')
        df = chloropleth.cross_join(firestations)

        df = df.with_column('DISTANCE',F.call_udf('st_distance',F.col('CENTROID'),F.call_udf('st_makepoint',F.col('LONGITUDE'),F.col('LATITUDE'))))\
                        .filter((F.col('DISTANCE')<RF2[1])&(F.col('DISTANCE')>RF2[0]))

        df = df.select('H3INDEX','WKT','TOTAL_INCIDENTS','TOTAL_CASUALTIES','LATITUDE','LONGITUDE','POSTCODE','LATCTR','LONCTR','FIRE_STATION','ADDRESS','DISTANCE')

        df2 = df.group_by('H3INDEX').agg(F.min('DISTANCE').alias('DISTANCE'))
       

        df = df.join(df2, (df['H3INDEX'] == df2['H3INDEX']) & (df['DISTANCE'] == df2['DISTANCE']),lsuffix="", rsuffix="_right")
        df = df.select('H3INDEX','WKT','TOTAL_INCIDENTS','LATITUDE','LONGITUDE','POSTCODE','TOTAL_CASUALTIES','LATCTR','LONCTR','FIRE_STATION','ADDRESS','DISTANCE')
            
            
        return df.to_pandas()




    fdata = RADIUS_firestation_agg(S_YEAR,FIRE_SERVICE,RF2)
    fstatloc = pd.DataFrame()
    fstatloc['LATITUDE']=fdata['LATITUDE']
    fstatloc['LONGITUDE']=fdata['LONGITUDE']
    fstatloc = fstatloc.drop_duplicates().to_numpy()

    fstatloc2 = pd.DataFrame()
    fstatloc2['FIRE_STATION_NAME']=fdata['FIRE_STATION']
    fstatloc2['ADDRESS']=fdata['ADDRESS']
    fstatloc2['POSTCODE']=fdata['POSTCODE']
    fstatloc2 = fstatloc2.drop_duplicates().to_numpy()

    m2 = folium.Map(location=[fdata.LATCTR.iloc[0],fdata.LONCTR.iloc[0]], zoom_start=9.5, tiles="openstreetmap")

        
    geodframe = gpd.GeoDataFrame(RADIUS_firestation_agg(S_YEAR,FIRE_SERVICE,RF2))
        
    geodframe = geodframe.set_geometry(gpd.GeoSeries.from_wkt(geodframe['WKT'])) 
    geojson = gpd.GeoSeries(geodframe.set_index('H3INDEX')['geometry']).to_json()
    
       

    folium.Choropleth(
        geo_data = geojson,
        name = 'Vehicle Incidents',
        data = geodframe,
        columns = ['H3INDEX','TOTAL_INCIDENTS'],
        key_on = 'feature.id',
        fill_color = 'OrRd',
        fill_opacity = 0.9,
        line_opacity = 0,
        legend_name = 'TOTAL INCIDENTS',
        smooth_factor = 0
        ).add_to(m2)
    

    folium.Choropleth(
            geo_data = geojson,
            name = 'Vehicle Casualties',
            data = geodframe,
            columns = ['H3INDEX','TOTAL_CASUALTIES'],
            key_on = 'feature.id',
            fill_color = 'BrBG',
            fill_opacity = 0.9,
            line_opacity = 0.4,
            legend_name = 'TOTAL CASUALTIES',
            smooth_factor = 0
        ).add_to(m2)
    folium.LayerControl().add_to(m2)
    
    for A in range (0,fstatloc.shape[0]):

            html4 = f'''
            <body style="background-color:#F0F0F0;">
            <p style="font-family:verdana">
            <b> FIRE STATION DETAILS
            <BR><BR>
            <b>Fire Station Name: </b>{fstatloc2[A][0]}<BR><BR>
        
            <br><br><b>Address:</b>
            {fstatloc2[A][0]}<BR><BR>

            <hr>
        
            <p style="font-family:verdana">
        
            <b>STATS</b>
            <BR><BR>
            '''
            iframe4 = folium.IFrame(html4,width=700,height=400)
            popup4 = folium.Popup(iframe4,max_width=700)
        
        


            folium.Marker(fstatloc[A], popup=popup4, icon=folium.Icon(color='red', prefix='fa',icon='fire'), icon_size=(40,40)).add_to(m2)

    firemap,table = st.columns(2)
    with firemap:
        st_FIRE_data = folium_static(m2, width=500, height= 700)

    with table:
            dfrad = RADIUS_firestation_agg(S_YEAR,FIRE_SERVICE,RF2)
            firedata = pd.DataFrame()
            firedata['Fire Station'] = dfrad.FIRE_STATION
            firedata['Address'] = dfrad.ADDRESS
            #firedata['Distance'] = dfrad.DISTANCE
            firedata['Casualties'] = dfrad.TOTAL_CASUALTIES
            firedata['Incidents'] = dfrad.TOTAL_INCIDENTS
            st.dataframe(firedata,900,700)
    
    
    #this is an embedded tableau report - I am passing the measure and fireservice variables into the tableau filter and parameter



with firetab3:
    st.markdown ('#### UTLIZE A COMMON MACHINE LEARNING ALGORITHM TO PREDICT THE CHANCE OF AN ACCIDENT')

    @st.cache_data
    def police_list():
        return session.sql('''SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'police_force' ''').to_pandas()

    
    
    
    @st.cache_data
    def fire_station_list():
        return session.sql('select FIRE_SERVICE_CODE, FIRE_SERVICE_NAME, FIRE_STATION, ADDRESS, POSTCODE,LATITUDE,LONGITUDE, POINT FROM FIRE_STATIONS').to_pandas()

    fire_select = FIRE_SERVICE
    newdf = fire_station_list()[(fire_station_list().FIRE_SERVICE_NAME == fire_select)]
    
    
    
    with st.form("my_form"):
        select2, select3, select4 = st.columns(3)
        with select2:
            select_fire_station = st.selectbox("Select Fire Station",(newdf.FIRE_STATION))
    
        newdffs = newdf[newdf.FIRE_STATION == select_fire_station]

        with select3:
            choose_model = st.selectbox("Select Model Predictor",(['CASUALTY_PREDICTOR','HIGH_FATALITY_PREDICTOR']))
        with select4:
            choose_size = st.slider('''Select Number of Hexagons away from Fire Station''',1,5,1)


        predvar1, predvar2, predvar3 = st.columns(3)
        with predvar1:
            age_of_driver = st.slider('Increase / decrease ages of driver by X %?', 0.1, 4.0, 1.0)
        with predvar2:
            age_of_vehicle = st.slider('Increase / decrease ages of vehicle by X %?', 0.1, 4.0, 1.0)
        with predvar3:
            engine_size = st.slider('Increase / decrease engine_size by X %?', 0.1, 4.0, 1.0)
        submitted = st.form_submit_button("submit")

        
        
        #here i am utilising folium as well as matplot library to visualise the results inside of streamlit.  
        
        
        if submitted:

            #store the sql syntax
            # NB this could have quite easily have been a snowflake function or even a stored proc for creation of the prediction table
            sql = f'''SELECT POLICE_FORCE "Police Force", 
            H3INDEX,
            any_value(st_aswkt(ANALYTICS_TOOLBOX.CARTO.H3_BOUNDARY(H3INDEX))) WKT,
            VEHICLE_MANOEUVRE "Vehicle Manoeuvre", 
            ROAD_TYPE "Road Type", 
            VEHICLE_TYPE "Vehicle Type", 
            WEATHER_CONDITIONS "Weather Conditions",
            DAY_OF_WEEK "Day of Week", 
            AVG(ENGINE_CAPACITY_CC)::INT "Engine Capacity CC",
            AVG(ADJUSTED_ENGINE_SIZE)::INT "Adjusted Engine Capacity CC",
            AVG(AGE_OF_DRIVER)::INT "Age of Driver",
            AVG(ADJUSTED_AGE_OF_DRIVER)::INT "Adjusted age of Driver",
            AVG(AGE_OF_VEHICLE)::INT "Age of Vehicle", 
            AVG(ADJUSTED_AGE_OF_VEHICLE)::INT "Adjusted age of Vehicle",
            AVG(PREDICTED_FATALITY)::FLOAT PREDICTION 
    
            FROM

            (
            select RAW.BATCH_PREDICT_CASUALTIES(
            js_hextoint(A.H3INDEX)
            ,YEAR
            ,VEHICLE_MANOEUVRE
            ,DAY_OF_WEEK
            ,ROAD_TYPE
            ,SPEED_LIMIT
            ,WEATHER_CONDITIONS
            ,VEHICLE_TYPE
            ,AGE_OF_VEHICLE * {age_of_vehicle}::INT
            ,AGE_OF_DRIVER * {age_of_driver}::INT
            ,ENGINE_CAPACITY_CC * {engine_size}::INT



            )PREDICTED_FATALITY,
            A.H3INDEX,
            B.VALUE AS POLICE_FORCE, 
            C.VALUE AS VEHICLE_MANOEUVRE,
            D.VALUE AS ROAD_TYPE,
            E.VALUE AS VEHICLE_TYPE,
            F.VALUE AS WEATHER_CONDITIONS,
            G.VALUE AS DAY_OF_WEEK,
            AGE_OF_DRIVER,
            AGE_OF_VEHICLE,
            ENGINE_CAPACITY_CC,
            AGE_OF_DRIVER * ({age_of_driver}) ADJUSTED_AGE_OF_DRIVER,
            AGE_OF_VEHICLE * ({age_of_vehicle}) ADJUSTED_AGE_OF_VEHICLE,
            ENGINE_CAPACITY_CC * ({engine_size}) ADJUSTED_ENGINE_SIZE

            from 


            (SELECT * FROM (SELECT * FROM accidents_with_perc_fatal where YEAR = {S_YEAR}))A 

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'police_force')B
            ON A.POLICE_FORCE = B.CODE

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'vehicle_manoeuvre')C
            ON A.VEHICLE_MANOEUVRE = C.CODE

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'road_type')D
            ON A.ROAD_TYPE = D.CODE

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'vehicle_type')E
            ON A.VEHICLE_TYPE = E.CODE

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'weather_conditions')F
            ON A.WEATHER_CONDITIONS = F.CODE

            INNER JOIN (SELECT CODE,VALUE FROM LOOKUPS WHERE FIELD = 'day_of_week')G
            ON A.DAY_OF_WEEK = G.CODE

            INNER JOIN (
                SELECT VALUE:index H3INDEX, VALUE:distance DISTANCE FROM 

                (SELECT FIRE_STATION,  ANALYTICS_TOOLBOX.CARTO.H3_KRING_DISTANCES(ANALYTICS_TOOLBOX.CARTO.H3_FROMGEOGPOINT(POINT, 7), {choose_size})HEXAGONS 

                FROM  (SELECT * FROM FIRE_STATIONS WHERE FIRE_STATION = '{select_fire_station}')),lateral flatten (input =>HEXAGONS)) H

                ON A.H3INDEX = H.H3INDEX


        

            
        


            )
    

            GROUP BY POLICE_FORCE, VEHICLE_MANOEUVRE, ROAD_TYPE, VEHICLE_TYPE, WEATHER_CONDITIONS, DAY_OF_WEEK, H3INDEX

            '''

    
            @st.cache_data(max_entries=2)
            def predictions(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model):
                df = session.sql(sql)
                df.write.mode("overwrite").save_as_table("predictions.Casualty_Predictions")
            
                return  print ('table written')
        
            with st.spinner('Please wait for casualty predictions...'):
                predictions(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model)
            st.success('Predictions complete', icon="✅")
        

            @st.cache_data(max_entries=2)
            def preddet(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model):
                df = session.table('predictions.Casualty_Predictions')
                return df.to_pandas()


            @st.cache_data(max_entries=2)
            def predict(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model):
                df = session.table('predictions.Casualty_Predictions')
                return df.group_by('H3INDEX','WKT').agg(F.sum('PREDICTION').alias('PREDICTION')).to_pandas()

            #for charts
            p2 = preddet(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model)

            #for folium
            p = predict(choose_size, select_fire_station, age_of_driver,age_of_vehicle,engine_size,choose_model)
        
        
            #st.write(newdf[newdf.FIRE_STATION == select_fire_station])
        
            m11 = folium.Map(location=[newdffs.LATITUDE.iloc[0],newdffs.LONGITUDE.iloc[0]], zoom_start=9.5, tiles="openstreetmap")
            folium.Marker(location=[newdffs.LATITUDE.iloc[0],newdffs.LONGITUDE.iloc[0]],icon=folium.Icon(color='red', prefix='fa',icon='fire')).add_to(m11)

            pgeodframe = gpd.GeoDataFrame(p)

            geodframe = geodframe.set_geometry(gpd.GeoSeries.from_wkt(geodframe['WKT'])) 
            geojson = gpd.GeoSeries(geodframe.set_index('H3INDEX')['geometry']).to_json()



            hexbin = folium.Choropleth(
                geo_data = geojson,
                name = 'Vehicle Incidents',
                data = pgeodframe,
                columns = ['H3INDEX','PREDICTION'],
                key_on = 'feature.id',
                fill_color = 'OrRd',
                fill_opacity = 0.9,
                line_opacity = 0.5,
                legend_name = 'Prediction',
                smooth_factor = 0
                ).add_to(m11)

       
        
        
            st.metric(label = "Total Predicted Casualties", value=round(p2.PREDICTION.sum(),2))
            colmap,coldata = st.columns(2)
            with colmap:
                st_casualty_predictions = folium_static(m11, width=500, height= 700)

            with coldata:
                coldata1, coldata2 = st.columns(2)
                with coldata1: 
                    st.markdown('###### CASUALTIES PER ROAD TYPE')
                    st.bar_chart(p2.groupby('Road Type')['PREDICTION'].sum())

                    st.markdown('###### CASUALTIES BY DAY OF WEEK')
                    st.bar_chart(p2.groupby('Day of Week')['PREDICTION'].sum())
                

                with coldata2:
                    st.markdown('###### CASUALTIES PER MANOEUVRE')
                    st.bar_chart(p2.groupby('Vehicle Manoeuvre')['PREDICTION'].sum())

                    st.markdown('###### CASUALTIES PER WEATHER CONDITION')
                    st.bar_chart(p2.groupby('Weather Conditions')['PREDICTION'].sum())
            
                st.bar_chart(p2.groupby('H3INDEX')['PREDICTION'].sum())
                

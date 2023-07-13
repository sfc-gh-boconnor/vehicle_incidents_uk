import streamlit as st



st.set_page_config(
    page_title="Vehicle Incidents in England",
    page_icon=":pickup_truck:",
    initial_sidebar_state="expanded",
)

st.write("# VEHICLE INCIDENTS IN ENGLAND :pickup_truck:")

st.sidebar.success("Select a demo above.")

st.markdown(
    """##### We are exploring Vehicle incidents accross england whilst utilising the capabilities of Streamlit, Snowflake, Carto and Tableau.
    
Click through the pages to view all towns in England based on real data, then see how this relates to Integrated Care Boards.

Explore The Details of your chosen City and see how this is indexed into the H3 geospatial grid (Hexagons). Carto's Toolkit shared within Snowflake has been utilised for this. You may also see this within a radius of your chosen city.

Finally, Explore How Vehicle incidents relate to fire service areas.

All the raw data has been curated, engineered and processed with Snowflake. There are also 3 dashboards created using Tableau Cloud to visualise off live Snowflake data. Folium and Matplotlib has also been used for some visuals. Enjoy :smile:


All code can be accessed here:

https://github.com/beckyoconnor/Vehicle_Incidents_uk/


"""
)
                

import streamlit as st
import snowflake.connector
import pandas as pd
from st_pages import Page, add_page_title, show_pages
import plotly.express as px
import altair as alt
import io
import geopandas as gpd
from vega_datasets import data
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import shapefile as shp
from shapely.geometry import Point
import numpy as np
sns.set_style('whitegrid')




def execute_query(query):
    try:
        conn = snowflake.connector.connect(
            account=st.session_state.account,
            role=st.session_state.role,
            warehouse=st.session_state.warehouse,
            database=st.session_state.database,
            schema=st.session_state.schema,
            user=st.session_state.user,
            password=st.session_state.password,
            client_session_keep_alive=True
        )
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Extract column names from cursor
        conn.close()
        result_df = pd.DataFrame(result, columns=columns)  # Create DataFrame with column names
        return result_df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

st.title('📊 SCHOOLS INFRA STATISTICS')

def main():

    Q1='''SELECT C.table_name, LISTAGG(C.column_name, ',') AS ALL_COLUMNS,T.COMMENT AS COMMENTS
            FROM information_schema.columns C
            inner join information_schema.tables T ON (T.table_name=C.table_name)
            WHERE C.table_schema = 'IND_SCHEMA' AND (C.table_name LIKE 'SCLS_WITH_%')
            GROUP BY C.table_name,COMMENTS
            ORDER BY C.table_name'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)


    st.divider()
    st.title("1.Schooll INFRA stats from 2013-14 to 2015-16")

    col1,col2,col3=st.columns(3)

    with col1:
            infra_s_year_options = ["2013-14", "2014-15","2015-16"]
            infra_s_year_index = infra_s_year_options.index("2013-14")
            infra_s_year = st.selectbox('Select which year:', options=infra_s_year_options, index=infra_s_year_index)
    with col2:
            infra_s_col_options = ["PRIMARY_ONLY",
                                    "PRIMARY_WITH_U_PRIMARY",
                                    "PRIMARY_WITH_U_PRIMARY_SEC_HRSEC",
                                    "U_PRIMARY_ONLY",
                                    "U_PRIMARY_WITH_SEC_HRSEC",
                                    "PRIMARY_WITH_U_PRIMARY_SEC",
                                    "U_PRIMARY_WITH_SEC",
                                    "SEC_ONLY",
                                    "SEC_WITH_HRSEC",
                                    "HRSEC_ONLY",
                                    "ALL_SCHOOLS"
                                    ]
            infra_s_col_index = infra_s_col_options.index("PRIMARY_ONLY")
            infra_s_col = st.selectbox('Select class:', options=infra_s_col_options, index=infra_s_col_index)
    with col3: 
            infra_f_options = ["TOILET","WATER","ELECTRICITY","GIRLS_TOILET","BOYS_TOILET","COMPUTER"] 
            infra_f_col_index = infra_f_options.index("TOILET") 
            infra_f_col = st.selectbox('Select infra facility:', options=infra_f_options, index=infra_f_col_index)
    # st.write(infra_f_col) 
    selected_items = f"Infra stat for facility: {infra_f_col}  Year: {infra_s_year}  Class: {infra_s_col}"
    st.title(selected_items)
    Q2 = f''' WITH CTE AS 
        (SELECT STATES, YEAR, ROUND(IFNULL(TRY_TO_DOUBLE("{infra_s_col}"), 0), 2) AS INFRA_PERCENTAGE
            FROM V01_SCLS_WITH_INFRA_2014_2016 WHERE YEAR ='{infra_s_year}' AND INFRA='{infra_f_col}'
            )           
        SELECT INDIA_STATES.STATES,CTE.INFRA_PERCENTAGE  FROM INDIA_STATES 
            LEFT JOIN CTE ON (CTE.STATES=INDIA_STATES.STATES) '''
    R2 = execute_query(Q2)

    r2_expander = st.expander("Data sets used in this analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)
    # selected_items = f"Infra stat for facility:{infra_f_col} Year: {infra_s_year}  Class: {infra_s_col}"
    # st.title(selected_items)

    india_states_shp = 'https://github.com/97Danu/Maps_with_python/raw/master/india-polygon.shp'
    india_states = gpd.read_file(india_states_shp)
    # st.write(india_states)
    india_states.rename(columns={'st_nm': 'STATES'}, inplace=True)

    # Merge dropout rates with GeoDataFrame
    merged_data = india_states.merge(R2_DF, how='left', on='STATES')

    # Define bins and labels
    bins = [float('-inf'), 50, 100, float('inf')]
    labels = ['Below 50', '50 - 100', 'Above 100']

    # Assigning values to bins and handling 'NA' values
    conditions = [
        merged_data['INFRA_PERCENTAGE'] < 50,
        (merged_data['INFRA_PERCENTAGE'] >= 50) & (merged_data['INFRA_PERCENTAGE'] <= 100),
        merged_data['INFRA_PERCENTAGE'] > 100
    ]

    # Assigning labels
    merged_data['color'] = np.select(conditions, labels, default='NA')

    # Create a plotly figure with categorical colors
    fig = px.choropleth_mapbox(merged_data, geojson=merged_data.geometry, locations=merged_data.index,
                            color='color',
                            color_discrete_map={'Below 50': 'Green', '50 - 100': 'Blue', 'Above 100': 'Red', 'NA': 'Yellow'},
                            mapbox_style="carto-positron",
                            hover_data={'STATES': True, 'INFRA_PERCENTAGE': True},
                            center={"lat": 20.5937, "lon": 78.9629},
                            zoom=3,
                            opacity=0.5)
    

# Update layout for better visualization
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, mapbox={'center': {'lat': 20, 'lon': 78}})

# Display the map in Streamlit
    st.plotly_chart(fig)

    st.markdown("""---------------------------------""")
    st.title("2.Top INFRA stats from 2013-14 to 2015-16")

    top_options = list(range(1, 31))  # Generates a list from 1 to 30
    top = st.selectbox('Select top INFRA_PERCENTAGE:', options=top_options, index=15)
    
    Q3 = f''' WITH CTE AS 
            (SELECT STATES, YEAR, ROUND(IFNULL(TRY_TO_DOUBLE("{infra_s_col}"), 0), 2) AS INFRA_PERCENTAGE, 
                DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY INFRA_PERCENTAGE DESC) DNK 
                FROM V01_SCLS_WITH_INFRA_2014_2016 where  YEAR ='{infra_s_year}' AND INFRA='{infra_f_col}')
                SELECT CTE.STATES,CTE.YEAR,CTE.INFRA_PERCENTAGE , CTE.DNK RANK FROM CTE where DNK <=   {top}  '''
    R3 = execute_query(Q3)
    r3_expander = st.expander("Data sets used in this analysis")
    R3_DF = pd.DataFrame(R3)
    R3_DF.index = R3_DF.index + 1
    r3_expander.write(R3_DF)
    R3_DF = R3_DF.sort_values(by="INFRA_PERCENTAGE", ascending=False)
    selected_items = f"Top  {top} Infra stat for facility: {infra_f_col}  Year: {infra_s_year}  Class: {infra_s_col}"
    # Creating the Altair chart
    chart = (
        alt.Chart(R3_DF)
        .mark_bar()
        .encode(
            x=alt.X("INFRA_PERCENTAGE:Q", title="INFRA PERCENTAGE"),
            y=alt.Y("STATES:N", title="States", sort="-x"),
            tooltip=[
            alt.Tooltip("STATES", title="State"),
            alt.Tooltip("INFRA_PERCENTAGE", title="INFRA PERCENTAGE"),
            ]
        )
        .properties(width=800,  title=f"{selected_items}")
        .interactive()
    )

        # Displaying the chart using Streamlit
    st.altair_chart(chart, use_container_width=True)
 
    st.markdown("""---------------------------------""")
    
    

 

                    
if __name__ == "__main__":
    main()
footer="""<style>
a:link , a:visited{
color: blue;
background-color: transparent;
text-decoration: underline;
}

a:hover,  a:active {
color: red;
background-color: transparent;
text-decoration: underline;
}

.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: white;
color: black;
text-align: center;
}
</style>
<div class="footer">
<p>Developed with ❤️ by <a style='display: inline; text-align: center;' href="https://www.linkedin.com/in/danammahiremath/" target="_blank">DANAMMA HIREMATH</a></p>
</div>
"""
st.markdown(footer,unsafe_allow_html=True)  
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

st.title('📊 ALL INDIA SURVEY ON HIGHER EDUCATION')

def main():

    Q1='''SELECT C.table_name, LISTAGG(C.column_name, ',') AS ALL_COLUMNS,T.COMMENT AS COMMENTS
            FROM information_schema.columns C
            inner join information_schema.tables T ON (T.table_name=C.table_name)
            WHERE C.table_schema = 'IND_SCHEMA' AND  (C.table_name LIKE 'AISHE_%')
            GROUP BY C.table_name,COMMENTS
            ORDER BY C.table_name'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)


    st.divider()
    st.title("1.Number of colleges,Institutes and across India in 2015-16")

    ais_edu_options = ["Colleges",  "Standalone Institutes" ,"Universities" ]
    ais_edu_index = ais_edu_options.index("Colleges")
    ais_edu= st.selectbox('Select Education type:', options=ais_edu_options, index=ais_edu_index)
    selected_items = f"Education type: {ais_edu} "
    st.title(selected_items)
    Q2 = f''' WITH CTE AS 
                (SELECT STATES, YEAR,COUNT 
                    FROM V01_AISHE_EDUCATION_TYPE WHERE  EDUCATION_TYPE= '{ais_edu}' 
                    )           
                SELECT INDIA_STATES.STATES,CTE.COUNT  FROM INDIA_STATES 
                    LEFT JOIN CTE ON (CTE.STATES=INDIA_STATES.STATES) '''
    R2 = execute_query(Q2)

    r2_expander = st.expander("Data sets used in this analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)

    india_states_shp = 'https://github.com/97Danu/Maps_with_python/raw/master/india-polygon.shp'
    india_states = gpd.read_file(india_states_shp)
    india_states.rename(columns={'st_nm': 'STATES'}, inplace=True)

    # Merge dropout rates with GeoDataFrame
    merged_data = india_states.merge(R2_DF, how='left', on='STATES')

    # Define bins and labels
    bins = [float('-inf'), 100, 1000, float('inf')]
    labels = ['Below 100', '100 - 1000', 'Above 1000']

    # Assigning values to bins and handling 'NA' values
    conditions = [
        merged_data['COUNT'] < 100,
        (merged_data['COUNT'] >= 100) & (merged_data['COUNT'] <= 1000),
        merged_data['COUNT'] > 1000
    ]

    # Assigning labels
    merged_data['color'] = np.select(conditions, labels, default='NA')

    # Create a plotly figure with categorical colors
    fig = px.choropleth_mapbox(merged_data, geojson=merged_data.geometry, locations=merged_data.index,
                            color='color',
                            color_discrete_map={'Below 100': 'Green', '100 - 1000': 'Blue', 'Above 1000': 'Red', 'NA': 'Yellow'},
                            mapbox_style="carto-positron",
                            hover_data={'STATES': True, 'COUNT': True},
                            center={"lat": 20.5937, "lon": 78.9629},
                            zoom=3,
                            opacity=0.5)
    

# Update layout for better visualization
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, mapbox={'center': {'lat': 20, 'lon': 78}})

# Display the map in Streamlit
    st.plotly_chart(fig)

    st.markdown("""---------------------------------""")
    st.title("2.Top count of colleges,Institutes and across India in 2015-16")

    top_options = list(range(1, 31))  # Generates a list from 1 to 30
    top = st.selectbox('Select top COUNT:', options=top_options, index=15)
    
    Q3 = f'''  WITH CTE AS 
            (SELECT STATES, YEAR,COUNT, 
                DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY COUNT DESC) DNK 
                FROM V01_AISHE_EDUCATION_TYPE WHERE  EDUCATION_TYPE = '{ais_edu}' )
                SELECT CTE.STATES,CTE.YEAR,CTE.COUNT , CTE.DNK RANK FROM CTE where DNK <= {top}  '''
    R3 = execute_query(Q3)
    r3_expander = st.expander("Data sets used in this analysis")
    R3_DF = pd.DataFrame(R3)
    R3_DF.index = R3_DF.index + 1
    r3_expander.write(R3_DF)
    R3_DF = R3_DF.sort_values(by="COUNT", ascending=False)
    selected_items = f"Top  {top}  Education type : {ais_edu} in India  Year: 2015-16"
    # Creating the Altair chart
    chart = (
        alt.Chart(R3_DF)
        .mark_bar()
        .encode(
            x=alt.X("COUNT:Q", title="COUNT"),
            y=alt.Y("STATES:N", title="States", sort="-x"),
            tooltip=[
            alt.Tooltip("STATES", title="State"),
            alt.Tooltip("COUNT", title="COUNT"),
            ]
        )
        .properties(width=800,  title=f"{selected_items}")
        .interactive()
    )

        # Displaying the chart using Streamlit
    st.altair_chart(chart, use_container_width=True)
 
    st.markdown("""---------------------------------""")
    st.title("3.Education loan distribution stats across India in 2015-16")

    col5,col6=st.columns(2)
    with col5:
        ais_loan_options = ["Standalone Institutes" ,"Universities" ]
        ais_loan_index = ais_loan_options.index("Universities")
        ais_loan_edu= st.selectbox('Select Education type:', options=ais_loan_options, index=ais_loan_index)

    with col6:
        ais_loan_col_options = ["TOTAL_GENERAL_TOTAL", "TOTAL_GENERAL_FEMALES",
                                "TOTAL_SC_TOTAL", "TOTAL_SC_FEMALES",
                                "TOTAL_ST_TOTAL", "TOTAL_ST_FEMALES",
                                "TOTAL_OBC_TOTAL", "TOTAL_OBC_FEMALES",
                                "TOTAL_TOTAL_PERSONS", "TOTAL_TOTAL_FEMALES",
                                "PWD_GENERAL_TOTAL", "PWD_GENERAL_FEMALES",
                                "PWD_SC_TOTAL", "PWD_SC_FEMALES",
                                "PWD_ST_TOTAL", "PWD_ST_FEMALES",
                                "PWD_OBC_TOTAL", "PWD_OBC_FEMALES",
                                "PWD_TOTAL_PERSONS", "PWD_TOTAL_FEMALES",
                                "MUSLIM_MINORITY_GENERAL_TOTAL", "MUSLIM_MINORITY_GENERAL_FEMALES",
                                "MUSLIM_MINORITY_SC_TOTAL", "MUSLIM_MINORITY_SC_FEMALES",
                                "MUSLIM_MINORITY_ST_TOTAL", "MUSLIM_MINORITY_ST_FEMALES",
                                "MUSLIM_MINORITY_OBC_TOTAL", "MUSLIM_MINORITY_OBC_FEMALES",
                                "MUSLIM_MINORITY_TOTAL_PERSONS", "MUSLIM_MINORITY_TOTAL_FEMALES",
                                "OTHER_MINORITY_GENERAL_TOTAL", "OTHER_MINORITY_GENERAL_FEMALES",
                                "OTHER_MINORITY_SC_TOTAL", "OTHER_MINORITY_SC_FEMALES",
                                "OTHER_MINORITY_ST_TOTAL", "OTHER_MINORITY_ST_FEMALES",
                                "OTHER_MINORITY_OBC_TOTAL", "OTHER_MINORITY_OBC_FEMALES",
                                "OTHER_MINORITY_TOTAL_PERSONS", "OTHER_MINORITY_TOTAL_FEMALES"]
        ais_loan_col_index = ais_loan_col_options.index("PWD_TOTAL_FEMALES")
        ais_loan_col = st.selectbox('Select cast of category :', options=ais_loan_col_options, index=ais_loan_col_index)
    selected_items = f"Education loan distribution for Education type: {ais_loan_edu}  category: {ais_loan_col}  in India  Year: 2015-16"
    st.title(selected_items)
    Q3 = f''' WITH CTE AS 
                (SELECT STATES,"{ais_loan_col}" LOAN_COUNT 
                    FROM V01_AISHE_EDUCATION_TYPE_LOANS_2015_2016 WHERE  EDUCATION_TYPE= '{ais_loan_edu}' 
                    )           
                SELECT INDIA_STATES.STATES,CTE.LOAN_COUNT  FROM INDIA_STATES 
                    LEFT JOIN CTE ON (CTE.STATES=INDIA_STATES.STATES)'''
    R3 = execute_query(Q3)

    r3_expander = st.expander("Data sets used in this analysis")
    R3_DF = pd.DataFrame(R3)
    R3_DF.index = R3_DF.index + 1
    r3_expander.write(R3_DF)

    india_states_shp = 'https://github.com/97Danu/Maps_with_python/raw/master/india-polygon.shp'
    india_states = gpd.read_file(india_states_shp)
    india_states.rename(columns={'st_nm': 'STATES'}, inplace=True)

    # Merge dropout rates with GeoDataFrame
    merged_data = india_states.merge(R3_DF, how='left', on='STATES')

    # Define bins and labels
    bins = [float('-inf'), 100, 1000, float('inf')]
    labels = ['Below 100', '100 - 1000', 'Above 1000']

    # Assigning values to bins and handling 'NA' values
    conditions = [
        merged_data['LOAN_COUNT'] < 100,
        (merged_data['LOAN_COUNT'] >= 100) & (merged_data['LOAN_COUNT'] <= 1000),
        merged_data['LOAN_COUNT'] > 1000
    ]

    # Assigning labels
    merged_data['color'] = np.select(conditions, labels, default='NA')

    # Create a plotly figure with categorical colors
    fig = px.choropleth_mapbox(merged_data, geojson=merged_data.geometry, locations=merged_data.index,
                            color='color',
                            color_discrete_map={'Below 100': 'Green', '100 - 1000': 'Blue', 'Above 1000': 'Red', 'NA': 'Yellow'},
                            mapbox_style="carto-positron",
                            hover_data={'STATES': True, 'LOAN_COUNT': True},
                            center={"lat": 20.5937, "lon": 78.9629},
                            zoom=3,
                            opacity=0.5)
    

# Update layout for better visualization
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, mapbox={'center': {'lat': 20, 'lon': 78}})

# Display the map in Streamlit
    st.plotly_chart(fig)

    st.markdown("""---------------------------------""")
    
    st.title("4.Top states based on education loan distribution across India in 2015-16")

    top_loan_options = list(range(1, 10))  # Generates a list from 1 to 30
    top_loan = st.selectbox('Select top LOAN_COUNT:', options=top_loan_options, index=9)
    
    Q4 = f'''   WITH CTE AS 
            (SELECT STATES,"{ais_loan_col}" LOAN_COUNT, 
                DENSE_RANK() OVER ( ORDER BY LOAN_COUNT DESC) DNK 
                FROM  V01_AISHE_EDUCATION_TYPE_LOANS_2015_2016 WHERE  EDUCATION_TYPE= '{ais_loan_edu}' )
                SELECT CTE.STATES,CTE.LOAN_COUNT, CTE.DNK RANK FROM CTE where DNK <= 10  '''
    R4 = execute_query(Q4)
    r4_expander = st.expander("Data sets used in this analysis")
    R4_DF = pd.DataFrame(R4)
    R4_DF.index = R4_DF.index + 1
    r4_expander.write(R4_DF)
    R4_DF = R4_DF.sort_values(by="LOAN_COUNT", ascending=False)
    selected_items = f"Top  {top_loan}  states for edu loan Education type : {ais_loan_edu} category: {ais_loan_col}  in India  Year: 2015-16"
    # Creating the Altair chart
    chart = (
        alt.Chart(R4_DF)
        .mark_bar()
        .encode(
            x=alt.X("LOAN_COUNT:Q", title="LOAN_COUNT"),
            y=alt.Y("STATES:N", title="States", sort="-x"),
            tooltip=[
            alt.Tooltip("STATES", title="State"),
            alt.Tooltip("LOAN_COUNT", title="LOAN_COUNT"),
            ]
        )
        .properties(width=800,  title=f"{selected_items}")
        .interactive()
    )

        # Displaying the chart using Streamlit
    st.altair_chart(chart, use_container_width=True)
    

 

                    
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
import streamlit as st
import snowflake.connector
from main import *
from st_pages import Page, add_page_title, show_pages
# import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt
import io
import geopandas as gpd
from vega_datasets import data
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import shapefile as shp
from shapely.geometry import Point
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

st.title('CLASSWISE & AGEWISE ENROLLMENT TO SCHOOLS')

def main():

    Q1='''SELECT C.table_name, LISTAGG(C.column_name, ',') AS ALL_COLUMNS,T.COMMENT AS COMMENTS
            FROM information_schema.columns C
            inner join information_schema.tables T ON (T.table_name=C.table_name)
            WHERE C.table_schema = 'IND_SCHEMA' AND (C.table_name LIKE 'CLSWISE_ENR_%' or C.table_name LIKE 'ENRL_BY_AGE_%')
            GROUP BY C.table_name,COMMENTS
            ORDER BY C.table_name'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)


    st.divider()
    st.title("1.Gross Enrolment Ratio from 2013-14 to 2015-16")

    col1,col2=st.columns(2)

    with col1:
            enr_s_year_options = ["2013-14", "2015-16", "2014-15"]
            enr_s_year_index = enr_s_year_options.index("2013-14")
            enr_s_year = st.selectbox('Select which year:', options=enr_s_year_options, index=enr_s_year_index)
    with col2:
            enr_s_col_options = ["PRIMARY_BOYS", "PRIMARY_GIRLS", "PRIMARY_TOTAL", "UPPER_PRIMARY_BOYS", "UPPER_PRIMARY_GIRLS",
                                "UPPER_PRIMARY_TOTAL", "SECONDARY_BOYS", "SECONDARY_GIRLS", "SECONDARY_TOTAL",
                                "HIGHER_SECONDARY_BOYS", "HIGHER_SECONDARY_GIRLS", "HIGHER_SECONDARY_TOTAL"]
            enr_s_col_index = enr_s_col_options.index("PRIMARY_BOYS")
            enr_s_col = st.selectbox('Select class:', options=enr_s_col_options, index=enr_s_col_index)
        
    Q2 = f''' WITH CTE AS 
        (SELECT STATES, YEAR, ROUND(IFNULL(TRY_TO_DOUBLE("{enr_s_col}"), 0), 2) AS GROSS_ENRL_RATIO
            FROM V01_ENRL_BY_GROSS_RATIO_2013_2015 WHERE YEAR ='{enr_s_year}'
            )           
        SELECT INDIA_STATES.STATES,CTE.GROSS_ENRL_RATIO FROM INDIA_STATES 
            LEFT JOIN CTE ON (CTE.STATES=INDIA_STATES.STATES) '''
    R2 = execute_query(Q2)

    r2_expander = st.expander("Data sets used in this analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)
    selected_items = f"Gross Enrolment Ratio for Year: {enr_s_year}  Class: {enr_s_col}"
    st.title(selected_items)

    india_states_shp = 'https://github.com/97Danu/Maps_with_python/raw/master/india-polygon.shp'
    india_states = gpd.read_file(india_states_shp)
    # st.write(india_states)
    india_states.rename(columns={'st_nm': 'STATES'}, inplace=True)

    # Merge dropout rates with GeoDataFrame
    merged_data = india_states.merge(R2_DF, how='left', on='STATES')

    # Define color ranges based on dropout rates
    color_categories = pd.cut(
        merged_data['GROSS_ENRL_RATIO'],
        bins=[float('-inf'), 95, 105, float('inf')],
        labels=['Below 95', '95 - 105', 'Above 105']
    )

    # Assign colors to each category
    color_dict = {
        'Below 95': 'Green',
        '95 - 105': 'Blue',
        'Above 105': 'green',
        'NaN or Missing': 'white'  # Category for NaN or missing values
    }

    # Map colors to each category
    merged_data['color'] = color_categories.map(color_dict)
    # For states with missing or NaN dropout rates, assign 'NaN or Missing' color
    merged_data.loc[merged_data['GROSS_ENRL_RATIO'].isnull(), 'color'] = 'NaN or Missing'
    # Create a plotly figure with categorical colors
    fig = px.choropleth_mapbox(merged_data, geojson=merged_data.geometry, locations=merged_data.index,
                            color='color',
                            mapbox_style="carto-positron",
                            hover_data={'STATES': True, 'GROSS_ENRL_RATIO': True},
                            center={"lat": 20.5937, "lon": 78.9629},
                            zoom=3,
                            opacity=0.5)
    # Add text annotations for DROP_OUT_RATE and STATES inside the map
    for idx, row in merged_data.iterrows():
        fig.add_annotation(
        text=f"{row['STATES']}<br>Dropout Rate: {row['GROSS_ENRL_RATIO']}",
        x=row.geometry.centroid.x,
        y=row.geometry.centroid.y,
        showarrow=False,
        font=dict(size=10, color='black')
        )

    # Update layout for better visualization
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, mapbox={'center': {'lat': 20, 'lon': 78}})

    # Display the map in Streamlit
    st.plotly_chart(fig)

    st.markdown("""---------------------------------""")
    st.title("3. Drop-out Rate for selected state and classes across 2009 TO 2012")

    

 

                    
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
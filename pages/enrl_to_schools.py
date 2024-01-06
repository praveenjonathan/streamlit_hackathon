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
    (SELECT STATES, YEAR, ROUND(IFNULL(TRY_TO_DOUBLE("{enr_s_col}"), 0), 2) AS DROP_OUT_RATE
        FROM V01_ENRL_BY_GROSS_RATIO_2013_2015)
        SELECT CTE.STATES STATE,CTE.DROP_OUT_RATE  FROM CTE---,INDIA_STATES.LATITUDE,INDIA_STATES.LONGITUDE FROM CTE
            ----INNER JOIN INDIA_STATES ON (CTE.STATES=INDIA_STATES.STATES)
        WHERE YEAR = '{enr_s_year}'  '''
    R2 = execute_query(Q2)

    r2_expander = st.expander("Data sets used in this analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)
    selected_items = f"Gross Enrolment Ratio for Year: {enr_s_year}  Class: {enr_s_col}"
    st.title(selected_items)

    india_states_geojson_url = 'src/india.geojson'
    # Load GeoJSON file into GeoDataFrame
    india_states = gpd.read_file(india_states_geojson_url)
    st.write(india_states)

    # Merge dropout rates with GeoDataFrame
    merged_data = india_states.merge(R2_DF, how='left', on='STATE')
    st.write(merged_data)

    # Plotting the map
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    merged_data.plot(column='DROP_OUT_RATE', cmap='YlOrRd', linewidth=0.8, ax=ax, edgecolor='0.8', legend=True)
    ax.axis('off')

    # Display the map in Streamlit
    st.pyplot(fig)

    # fig = px.scatter_geo(
    #     R2_DF,
    #     locations='STATES',
    #     lat='LATITUDE',
    #     lon='LONGITUDE',
    #     color='DROP_OUT_RATE',
    #     hover_name='STATES',
    #     size='DROP_OUT_RATE',
    #     projection='mercator',  # Using mercator projection
    #     labels={'DROP_OUT_RATE': 'Dropout Rate (%)'}
    # )

    # fig.update_geos(
    # showcountries=True,  # Show country boundaries
    # countrycolor="Black",
    # showland=True,
    # landcolor="rgb(217, 217, 217)",
    # showocean=True,
    # oceancolor="LightBlue",
    # showcoastlines=True,
    # coastlinewidth=1,
    # coastlinecolor="Black",
    # )

    # fig.update_layout(
    #     title=selected_items,
    #     geo=dict(
    #         scope='asia',
    #         landcolor='rgb(217, 217, 217)',
    #     )
    # )

    # st.plotly_chart(fig)


    # base = alt.topo_feature('pages/India_State_Boundary.shp', 'objects.INDIA')
    # # Replace 'path_to_india_map_shapefile' with the correct path
    # base = alt.topo_feature('base', 'objects.INDIA')
    # india = gpd.read_file('base')

    # # Merge the map data with the retrieved dataset
    # merged_data = india.merge(R2_DF, how='left', left_on='StatesColumnName', right_on='STATES')

    # # Create the map using Altair
    # map_chart = alt.Chart(merged_data).mark_geoshape().encode(
    #     color=alt.Color('DROP_OUT_RATE:Q', title='Gross Enrolment Ratio'),
    #     tooltip=['STATES:N', 'DROP_OUT_RATE:Q']
    # ).properties(
    #     width=500,
    #     height=600,
    #     title=selected_items
    # ).project(type='identity')

    # # Display the map using Streamlit
    # st.altair_chart(map_chart, use_container_width=True)
    

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
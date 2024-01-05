import streamlit as st
import snowflake.connector
from main import *
from st_pages import Page, add_page_title, show_pages
# import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt




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

    s_states_options = pd.DataFrame(execute_query('SELECT DISTINCT STATES FROM V01_ENRL_BY_GROSS_RATIO_2013_2015'))
    s_states_col = st.selectbox('Select state:', options=s_states_options['STATES'].tolist())
    col1, col2,col3=st.columns(3)
    with col1:
       s_states_col = st.selectbox('Select state:', options=s_states_options['STATES'].tolist())
    with col2:
        s_year_options = ["2013-14","2015-16","2014-15"]
        s_year = st.selectbox('Select which year:', options=s_year_options, index=0)
    with col3:
        s_col_options = ["PRIMARY_BOYS",
                        "PRIMARY_GIRLS",
                        "PRIMARY_TOTAL",
                        "UPPER_PRIMARY_BOYS",
                        "UPPER_PRIMARY_GIRLS",
                        "UPPER_PRIMARY_TOTAL",
                        "SECONDARY_BOYS",
                        "SECONDARY_GIRLS",
                        "SECONDARY_TOTAL",
                        "HIGHER_SECONDARY_BOYS",
                        "HIGHER_SECONDARY_GIRLS",
                        "HIGHER_SECONDARY_TOTAL"] 
        s_col_index = s_col_options.index("PRIMARY_BOYS")  # Find the index of the default value
        s_col = st.selectbox('Select class:', options=s_col_options, index=s_col_index)
  
    Q2 = f'''WITH CTE AS 
        (SELECT STATES, YEAR, ROUND(IFNULL(TRY_TO_DOUBLE("{s_col}"), 0), 2) AS DROP_OUT_RATE
            FROM V01_ENRL_BY_GROSS_RATIO_2013_2015)
            SELECT CTE.STATES,CTE.YEAR,CTE.DROP_OUT_RATE  FROM CTE WHERE YEAR = {s_year}'''
    R2 = execute_query(Q2)
    #AS "{s_col}"
    r2_expander = st.expander("Data sets used in this analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R3_DF)
    R3_DF = R3_DF.sort_values(by="DROP_OUT_RATE", ascending=False)
    selected_items = f"Gross Enrolment Ratio from Year: {s_year} for Class: {s_col}"
    

    st.markdown("""---------------------------------""")
    st.title("3. Drop-out Rate for selected state and classes across 2009 TO 2012")

    # # Select state and display dataset used in the analysis
    # s_states_options = pd.DataFrame(execute_query('SELECT DISTINCT STATES FROM V01_DRR_STATEWISE_CLASSWISE_2009_2012'))
    # s_states_col = st.selectbox('Select state:', options=s_states_options['STATES'].tolist())
    # Q4 = f"SELECT * FROM V01_DRR_STATEWISE_CLASSWISE_2009_2012 WHERE STATES = '{s_states_col}'"
    # R4 = execute_query(Q4)
    # r4_expander = st.expander("Data sets used in this analysis")
    # R4_DF = pd.DataFrame(R4)
    # R4_DF.index = R4_DF.index + 1
    # r4_expander.write(R4_DF)

    # # Select categories
    # selected_categories = st.multiselect('Select categories:', options=(R4_DF.columns)[2:],default=["I-V BOYS","I-V GIRLS"])

    # # Filter data based on selected state and categories
    # filtered_data = R4_DF[['YEAR', 'STATES'] + selected_categories]
    # filtered_data = filtered_data[filtered_data['STATES'] == s_states_col]

    # # Melt the DataFrame for visualization
    # melted_df = filtered_data.melt(id_vars=['YEAR', 'STATES'], var_name='Category', value_name='Dropout Rate')
    # r4_title = f"Dropout Rates for {', '.join(selected_categories)} in {s_states_col}"

    # # Line chart using Altair
    # line_chart = (
    #     alt.Chart(melted_df)
    #     .mark_line(point=True)
    #     .encode(
    #         x='YEAR:N',
    #         y=alt.Y('Dropout Rate:Q', title='Dropout Rate'),
    #         color='Category:N',
    #         tooltip=['YEAR:N', 'STATES:N', 'Category:N', 'Dropout Rate:Q']
    #     )
    #     .properties( title=r4_title)
    #     .configure_legend(
    #     orient='left',
    #     title=None,
    #     labelFontSize=9)
    #     .interactive()
    # )

    # # Display the chart using Streamlit
    # st.altair_chart(line_chart, use_container_width=True)
    # st.markdown("""---------------------------------""")

    

    

 

                    
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
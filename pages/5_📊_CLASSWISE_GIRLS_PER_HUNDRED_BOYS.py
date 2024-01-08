import streamlit as st
import snowflake.connector
import pandas as pd
from st_pages import Page, add_page_title, show_pages
# import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt




# st.set_page_config( layout="wide")

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

st.title('📊 CLASSWISE_GIRLS_PER_HUNDRED_BOYS ')
# left_column, right_column = st.columns(2)

def main():

    Q1='''SELECT C.table_name, LISTAGG(C.column_name, ',') AS ALL_COLUMNS,T.COMMENT AS COMMENTS
        FROM information_schema.columns C
        inner join information_schema.tables T ON (T.table_name=C.table_name)
        WHERE C.table_schema = 'IND_SCHEMA' AND C.table_name LIKE 'GIRLS_PER_%'
        GROUP BY C.table_name,COMMENTS
        ORDER BY C.table_name'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data sets used in this entire analysis")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)
    st.divider()
    st.title("1.Drop out rates in India from 1960-61 to 2010-11")
    Q2='''  SELECT * FROM V01_GIRLS_PER_100_2008_2012
            where STATES='INDIA'
            ORDER BY 1'''
    R2 = execute_query(Q2)
    r2_expander = st.expander("Data set used in this  analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)
    st.write('Year-wise drop out rate for all categories')

    st.title('Education Trends in India')

    # Dropdown for selecting Category
    selected_category = st.selectbox('Select Category', R2_DF['CATEGORY'].unique())

    # Filter data based on selected category
    filtered_data = R2_DF[R2_DF['CATEGORY'] == selected_category]

    # Line Chart Visualization
    st.subheader('Trend of Classes Over Years')
    line_chart = alt.Chart(filtered_data).mark_line().encode(
        x='YEAR',
        y=alt.Y('CLASSES I-XII', title='Number of Classes'),
        color='STATES',
        tooltip=['YEAR', 'STATES', 'CLASSES I-XII']
    ).properties(
        width=600,
        height=400
    ).interactive()

    st.altair_chart(line_chart)

    # Insights in Markdown
    st.markdown('## Insights')
    st.markdown('- **Increase in Classes**: Across all categories (All Categories, SC, ST), there is a general trend of an increase in the number of classes from 2008 to 2012.')
    st.markdown('- **Disparity among Categories**: ST category shows a consistently lower number of classes compared to the other categories over the years.')
    st.markdown('- **Fluctuations in Specific Class Ranges**: There are fluctuations in specific class ranges over the years, indicating variations in educational development.')


 

                    
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
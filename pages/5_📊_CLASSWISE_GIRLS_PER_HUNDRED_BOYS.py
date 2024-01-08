import streamlit as st
import snowflake.connector
import pandas as pd
from st_pages import Page, add_page_title, show_pages
# import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt
import seaborn as sns





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

st.title('📊 CLASSWISE GIRLS PER HUNDRED BOYS ')


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
    st.title("1.Classwise girls per hundred boys in entire india from 2008 to 2012")
    Q2='''    SELECT * exclude STATES  FROM V01_GIRLS_PER_100_2008_2012
            where STATES='INDIA'
            ORDER BY 1'''
    R2 = execute_query(Q2)
    r2_expander = st.expander("Data set used in this  analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)

    # Multi-select for selecting Categories with default all selected
    selected_categories = st.multiselect('Select Categories', R2_DF['CATEGORY'].unique(), default=list(R2_DF['CATEGORY'].unique()))

    # Select for choosing a single class
    selected_class = st.selectbox('Select Class', R2_DF.columns[2:], index=0)

    # Filter data based on selected categories
    filtered_data = R2_DF[R2_DF['CATEGORY'].isin(selected_categories)]

    # Filter data based on selected class
    filtered_data = filtered_data[['YEAR', 'CATEGORY', selected_class]]

    # Melt the DataFrame for visualization
    filtered_data_melted = pd.melt(filtered_data, id_vars=['YEAR', 'CATEGORY'], value_vars=[selected_class], var_name='Class', value_name='Girls per Hundred Boys')

    # Line Chart Visualization with points
    st.subheader(f'Girls per Hundred Boys in {selected_class}')

    line_chart = alt.Chart(filtered_data_melted).mark_line(point=True).encode(
        x=alt.X('YEAR:O', axis=alt.Axis(format='d'), title='Year'),
        y='Girls per Hundred Boys',
        color='CATEGORY',
        tooltip=['YEAR', 'Girls per Hundred Boys']
    ).properties(
        width=900
    ).interactive()

    st.altair_chart(line_chart)

    # Insights in Markdown
    st.markdown('## Insights')
    st.markdown('- **Disparity Among Categories**: Selecting multiple categories shows varying trends in gender disparity across different educational categories.')
    st.markdown('- **Temporal Variations**: Over the years, fluctuations in the ratio of girls per hundred boys are observed among the selected categories.')
    st.markdown(f'- **Classwise Trends in {selected_class}**: Displays the gender ratio in the chosen class across different categories.')

    st.divider()
    st.title("2.Classwise girls per hundred boys in states from 2008 to 2012 across different catergory")
    
    col5,col6=st.columns(2)
    with col5:
        q3_states_options = pd.DataFrame(execute_query('SELECT DISTINCT STATES FROM V01_GIRLS_PER_100_2008_2012'))
        q3_selected_state = st.selectbox('Select State', q3_states_options, index=5)
    with col6:
        q3_category_options=["SC","ST","ALL_CAT"]
        q3_category_index = q3_category_options.index("ALL_CAT")
        q3_selected_category = st.selectbox('Select Category', q3_category_options, q3_category_index)
    q3_title=f' Classwise girls per hundred boys in State: {q3_selected_state} and category: {q3_selected_category} '
    
    Q3=f'''WITH CTE
            AS
            (
            SELECT * FROM V01_GIRLS_PER_100_2008_2012
            where STATES= '{q3_selected_state}' and CATEGORY= '{q3_selected_category}' )
            SELECT *  FROM CTE'''
    R3 = execute_query(Q3)
    r3_expander = st.expander("Data set used in this  analysis")
    R3_DF = pd.DataFrame(R3)
    R3_DF.index = R3_DF.index + 1
    r3_expander.write(R3_DF)

    def plot_chart(data, x_axis, y_axes):
        selected_columns = [x_axis] + y_axes
        melted_data = data.melt('YEAR', value_vars=y_axes, var_name='CATEGORY')
        
        chart = alt.Chart(melted_data).mark_line(point=True).encode(
            x=alt.X('YEAR:N', title='YEAR',axis=alt.Axis(grid=True)),
            y=alt.Y('value:Q', title=f'State {q3_selected_state} category: {q3_selected_category} ', axis=alt.Axis(grid=True)),
            color='CATEGORY:N',
            tooltip=['YEAR:N', alt.Tooltip('value:Q', title='girls per hundred', format='d'), 'CATEGORY:N']
        ).properties(
            width=600,
            height=400
        ).configure_legend(
        orient='left',
        title=None,
        labelFontSize=9
        ).configure_axis(grid=False).interactive()
        return chart

    class_columns = [col for col in R3_DF.columns if 'CLASSES' in col]
    default_selection = class_columns if class_columns else [list(R3_DF.columns)[1]]  
    # Selecting X-axis (Year) and multiple Y-axis columns
    x_axis_column ='YEAR'
    y_axis_columns = st.multiselect('Select Y-axis (Categories)', options=list(R3_DF.columns)[1:], default=default_selection)

    # Plotting the chart with multiple Y-axis columns
    st.altair_chart(plot_chart(R3_DF, x_axis_column, y_axis_columns), use_container_width=True)   


    
    st.divider()
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

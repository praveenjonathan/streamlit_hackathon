import streamlit as st
import snowflake.connector
from main import *
from st_pages import Page, add_page_title, show_pages
# import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt




st.set_page_config( layout="wide")

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

st.title('DROP OUT ANALYSIS ')
# left_column, right_column = st.columns(2)

def main():

    Q1='''SELECT C.table_name, LISTAGG(C.column_name, ',') AS ALL_COLUMNS,T.COMMENT AS COMMENTS
        FROM information_schema.columns C
        inner join information_schema.tables T ON (T.table_name=C.table_name)
        WHERE C.table_schema = 'IND_SCHEMA' AND C.table_name LIKE 'DRR_%'
        GROUP BY C.table_name,COMMENTS
        ORDER BY C.table_name'''
    R1 = execute_query(Q1)
    r1_expander = st.expander("Data set used in this analysis")
    R1_DF = pd.DataFrame(R1)
    R1_DF.index = R1_DF.index + 1
    r1_expander.write(R1_DF)
    st.divider()
    st.markdown("1.Drop out rates in India from 1960-61 to 2010-11")
    Q2='''SELECT * FROM V01_DRR_1960_TO_2011'''
    R2 = execute_query(Q2)
    r2_expander = st.expander("Data sets used in this entire analysis")
    R2_DF = pd.DataFrame(R2)
    R2_DF.index = R2_DF.index + 1
    r2_expander.write(R2_DF)
    st.write('Year-wise drop out rate for all categories')

    # Creating the chart with multiple Y-axis columns
    def plot_chart(data, x_axis, y_axes):
        selected_columns = [x_axis] + y_axes
        melted_data = data.melt('YEAR', value_vars=y_axes, var_name='CATEGORY')
        
        chart = alt.Chart(melted_data).mark_line(point=True).encode(
            x=alt.X('YEAR:N', title='YEAR'),
            y=alt.Y('value:Q', title='DROPOUT_RATE'),
            color='CATEGORY:N',
            tooltip=['YEAR:N', alt.Tooltip('value:Q', title='DROPOUT_RATE', format='.2f'), 'CATEGORY:N']
        ).properties(
            width=600,
            height=400
        ).configure_legend(
        orient='left',
        title=None,
        labelFontSize=8
    )
        return chart
    # Selecting X-axis (Year) and multiple Y-axis columns with 'GIRL' keyword
    girl_columns = [col for col in R2_DF.columns if 'GIRLS' in col]
    default_selection = girl_columns if girl_columns else [list(R2_DF.columns)[1]]  # If 'GIRL' columns exist, use them as default, else use the first column
    # Selecting X-axis (Year) and multiple Y-axis columns
    x_axis_column ='YEAR'
    y_axis_columns = st.multiselect('Select Y-axis (Categories)', options=list(R2_DF.columns)[1:], default=default_selection)#[list(R2_DF.columns)[1]])

    # Plotting the chart with multiple Y-axis columns
    st.altair_chart(plot_chart(R2_DF, x_axis_column, y_axis_columns), use_container_width=True)
    st.divider()

 

                    
if __name__ == "__main__":
    main()

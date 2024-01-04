import streamlit as st
import snowflake.connector
from main import *
from st_pages import Page, add_page_title, show_pages
import matplotlib.pyplot as plt
import plotly.express as px



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
    st.markdown("1st")
    Q2='''SELECT * FROM DRR_1960_TO_2011'''
    R2 = execute_query(Q2)
    
    R2_DF = pd.DataFrame(R2)
    st.write(R2_DF)
    # # Set 'Year' as index for plotting
    # df = pd.DataFrame(data)

    # Streamlit app
    st.title('Interactive Education Data Visualization')
    st.write("Interactive line chart showing education statistics over years")

    # Select columns for X-axis (except 'Year')
    selected_columns = st.multiselect('Select columns for X-axis', R2_DF.columns[1:])

    # Check if columns are selected for visualization
    if selected_columns:
        # Melt the DataFrame for Plotly
        df_melted = R2_DF.melt(id_vars=["Year"], value_vars=selected_columns, var_name='Category', value_name='Value')
        
        # Create an interactive line chart using Plotly Express
        fig = px.line(df_melted, x="Year", y="Value", color='Category', markers=True, title='Education Statistics')
        st.plotly_chart(fig)
    else:
        st.write("Please select at least one column for visualization.")

                    
if __name__ == "__main__":
    main()

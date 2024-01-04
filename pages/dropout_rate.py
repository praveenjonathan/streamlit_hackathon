import streamlit as st
import snowflake.connector
from main import *
from st_pages import Page, add_page_title, show_pages
import matplotlib.pyplot as plt


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

# Set the index to 'Year' column
    R2_DF.set_index('Year', inplace=True)

    # Streamlit app
    st.title('Visualization of Education Data')
    st.write("Line chart showing education statistics over years")

    # Select columns for X-axis (except 'Year')
    selected_columns = st.multiselect('Select columns for X-axis', R2_DF.columns[1:])

    # Plotting the line chart
    if selected_columns:
        fig, ax = plt.subplots()
        for column in selected_columns:
            ax.plot(R2_DF.index, R2_DF[column], marker='o', label=column)
        ax.set_xlabel('Year')
        ax.set_ylabel('Values')
        ax.set_title('Education Statistics')
        ax.legend()
        st.pyplot(fig)
    else:
        st.write("Please select at least one column for visualization.")
                    
if __name__ == "__main__":
    main()

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
    r1_expander.write(R1_DF)
    st.markdown("1st")
    Q2='''SELECT * FROM DRR_1960_TO_2011'''
    R2 = execute_query(Q2)
    
    R2_DF = pd.DataFrame(R2)
    st.write(R2_DF)
    # Set 'Year' as index for plotting
    R2_DF.set_index('Year', inplace=True)

    # Plotting the data
    plt.figure(figsize=(10, 6))
    for column in R2_DF.columns:
        plt.plot(R2_DF.index, R2_DF[column], marker='o', label=column)

    plt.title('Dropout Rates for Different Categories Over Time')
    plt.xlabel('Year')
    plt.ylabel('Dropout Rate')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # with left_column:
         
    #      st.markdown(""" 
    #             Table: Employee
    
    #             | Column Name | Type    |
    #             |-------------|---------|
    #             | id          | int     |
    #             | name        | varchar |
    #             | department  | varchar |
    #             | managerId   | int     |
    
    #             id is the primary key (column with unique values) for this table.
    #             Each row of this table indicates the name of an employee, their department, and the id of their manager.
    #             If managerId is null, then the employee does not have a manager.
    #             No employee will be the manager of themselves.
    
    #             Write a solution to find managers with at least five direct reports.
    
    #             Return the result table in any order.
    
    #             The result format is in the following example.
    
    #             **Example 1:**
    
    #             **Input:** 
    #             Employee table:
    #             | id  | name  | department | managerId |
    #             |-----|-------|------------|-----------|
    #             | 101 | John  | A          | null      |
    #             | 102 | Dan   | A          | 101       |
    #             | 103 | James | A          | 101       |
    #             | 104 | Amy   | A          | 101       |
    #             | 105 | Anne  | A          | 101       |
    #             | 106 | Ron   | B          | 101       |
    
    #             **Output:** 
    #             | name |
    #             |------|
    #             | John |

    #            ``` sql
    #             CREATE or REPLACE TABLE Employee (
    #                     id INT,
    #                     name VARCHAR(50),
    #                     department VARCHAR(50),
    #                     managerId INT
    #                 ) AS (
    #                     SELECT * FROM VALUES
    #                         (101, 'John', 'A', NULL),
    #                         (102, 'Dan', 'A', 101),
    #                         (103, 'James', 'A', 101),
    #                         (104, 'Amy', 'A', 101),
    #                         (105, 'Anne', 'A', 101),
    #                         (106, 'Ron', 'B', 101)
    #                         )


    #             ```
    #             """)
    
    # with right_column:
    #      query = st.text_area("Write query",height=250)
    #      if st.button("Execute Queries"):
    #           if query:
    #                with st.spinner("Executing all queries..."):
    #                     result = execute_query(query)
    #                     st.success("Query executed!")
    #                     result_df = pd.DataFrame(result)
    #                     st.write(result_df)
                        
                    
if __name__ == "__main__":
    main()

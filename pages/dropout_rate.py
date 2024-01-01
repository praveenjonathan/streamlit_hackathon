import streamlit as st
import snowflake.connector
# from pathlib import Path
# from snowflakesql import connect_to_snowflake, store_credentials
from main import *
from st_pages import Page, add_page_title, show_pages

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

st.title('LeetCode 570. Managers with at Least 5 Direct Reports')
left_column, right_column = st.columns(2)

def main():
    with left_column:
         
         st.markdown(""" 
                Table: Employee
    
                | Column Name | Type    |
                |-------------|---------|
                | id          | int     |
                | name        | varchar |
                | department  | varchar |
                | managerId   | int     |
    
                id is the primary key (column with unique values) for this table.
                Each row of this table indicates the name of an employee, their department, and the id of their manager.
                If managerId is null, then the employee does not have a manager.
                No employee will be the manager of themselves.
    
                Write a solution to find managers with at least five direct reports.
    
                Return the result table in any order.
    
                The result format is in the following example.
    
                **Example 1:**
    
                **Input:** 
                Employee table:
                | id  | name  | department | managerId |
                |-----|-------|------------|-----------|
                | 101 | John  | A          | null      |
                | 102 | Dan   | A          | 101       |
                | 103 | James | A          | 101       |
                | 104 | Amy   | A          | 101       |
                | 105 | Anne  | A          | 101       |
                | 106 | Ron   | B          | 101       |
    
                **Output:** 
                | name |
                |------|
                | John |

               ``` sql
                CREATE or REPLACE TABLE Employee (
                        id INT,
                        name VARCHAR(50),
                        department VARCHAR(50),
                        managerId INT
                    ) AS (
                        SELECT * FROM VALUES
                            (101, 'John', 'A', NULL),
                            (102, 'Dan', 'A', 101),
                            (103, 'James', 'A', 101),
                            (104, 'Amy', 'A', 101),
                            (105, 'Anne', 'A', 101),
                            (106, 'Ron', 'B', 101)
                            )


                ```
                """)
    
    with right_column:
         query = st.text_area("Write query",height=250)
         if st.button("Execute Queries"):
              if query:
                   with st.spinner("Executing all queries..."):
                        result = execute_query(query)
                        st.success("Query executed!")
                        result_df = pd.DataFrame(result)
                        st.write(result_df)
                        
                    
if __name__ == "__main__":
    main()

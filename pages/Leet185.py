import streamlit as st
import snowflake.connector
# from pathlib import Path
# from snowflakesql import connect_to_snowflake, store_credentials
from snowflakesql import *
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

st.title('LeetCode 185. Department Top Three Salaries')
left_column, right_column = st.columns(2)

def main():
    with left_column:
         
        st.markdown(""" 
                Table: Employee

                | Column Name  | Type    |
                |--------------|---------|
                | id           | int     |
                | name         | varchar |
                | salary       | int     |
                | departmentId | int     |

                id is the primary key (column with unique values) for this table.
                departmentId is a foreign key (reference column) of the ID from the Department table.
                Each row of this table indicates the ID, name, and salary of an employee. It also contains the ID of their department.

                Table: Department

                | Column Name | Type    |
                |-------------|---------|
                | id          | int     |
                | name        | varchar |

                id is the primary key (column with unique values) for this table.
                Each row of this table indicates the ID of a department and its name.

                A company's executives are interested in seeing who earns the most money in each of the company's departments. A high earner in a department is an employee who has a salary in the top three unique salaries for that department.

                Write a solution to find the employees who are high earners in each of the departments.

                Return the result table in any order.

                Example 1:

                Input:
                Employee table:
                | id | name  | salary | departmentId |
                |----|-------|--------|--------------|
                | 1  | Joe   | 85000  | 1            |
                | 2  | Henry | 80000  | 2            |
                | 3  | Sam   | 60000  | 2            |
                | 4  | Max   | 90000  | 1            |
                | 5  | Janet | 69000  | 1            |
                | 6  | Randy | 85000  | 1            |
                | 7  | Will  | 70000  | 1            |

                Department table:
                | id | name  |
                |----|-------|
                | 1  | IT    |
                | 2  | Sales |

                Output:
                | Department | Employee | Salary |
                |------------|----------|--------|
                | IT         | Max      | 90000  |
                | IT         | Joe      | 85000  |
                | IT         | Randy    | 85000  |
                | IT         | Will     | 70000  |
                | Sales      | Henry    | 80000  |
                | Sales      | Sam      | 60000  |

                Explanation:
                In the IT department:
                - Max earns the highest unique salary
                - Both Randy and Joe earn the second-highest unique salary
                - Will earns the third-highest unique salary

                In the Sales department:
                - Henry earns the highest salary
                - Sam earns the second-highest salary
                - There is no third-highest salary as there are only two employees


               ``` sql
                -- Create Employee table
                CREATE TABLE Employee (
                    id INT,
                    name VARCHAR(50),
                    salary INT,
                    departmentId INT,
                    PRIMARY KEY (id),
                    FOREIGN KEY (departmentId) REFERENCES Department(id)
                );

                -- Create Department table
                CREATE TABLE Department (
                    id INT,
                    name VARCHAR(50),
                    PRIMARY KEY (id)
                );

                -- Insert data into Department table
                INSERT INTO Department (id, name) VALUES
                    (1, 'IT'),
                    (2, 'Sales');

                -- Insert data into Employee table
                INSERT INTO Employee (id, name, salary, departmentId) VALUES
                    (1, 'Joe', 85000, 1),
                    (2, 'Henry', 80000, 2),
                    (3, 'Sam', 60000, 2),
                    (4, 'Max', 90000, 1),
                    (5, 'Janet', 69000, 1),
                    (6, 'Randy', 85000, 1),
                    (7, 'Will', 70000, 1);


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
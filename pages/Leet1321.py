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

st.title('LeetCode 1321. Restaurant Growth')
left_column, right_column = st.columns(2)

def main():
    with left_column:
         
        st.markdown(""" 
                Table: Customer

    
                | Column Name   | Type    |
                |---------------|---------|
                | customer_id   | int     |
                | name          | varchar |
                | visited_on    | date    |
                | amount        | int     |



                In SQL,(customer_id, visited_on) is the primary key for this table.
                This table contains data about customer transactions in a restaurant.
                visited_on is the date on which the customer with ID (customer_id) has visited the restaurant.
                amount is the total paid by a customer.
                

                You are the restaurant owner and you want to analyze a possible expansion (there will be at least one customer every day).

                Compute the moving average of how much the customer paid in a seven days window (i.e., current day  6 days before). average_amount should be rounded to two decimal places.

                Return the result table ordered by visited_on in ascending order.

                The result format is in the following example.

                

                Example 1:

                Input: 
                Customer table:
               
                | customer_id | name         | visited_on   | amount      |
                |-------------|--------------|--------------|-------------|
                | 1           | Jhon         | 2019-01-01   | 100         |
                | 2           | Daniel       | 2019-01-02   | 110         |
                | 3           | Jade         | 2019-01-03   | 120         |
                | 4           | Khaled       | 2019-01-04   | 130         |
                | 5           | Winston      | 2019-01-05   | 110         | 
                | 6           | Elvis        | 2019-01-06   | 140         | 
                | 7           | Anna         | 2019-01-07   | 150         |
                | 8           | Maria        | 2019-01-08   | 80          |
                | 9           | Jaze         | 2019-01-09   | 110         | 
                | 1           | Jhon         | 2019-01-10   | 130         | 
                | 3           | Jade         | 2019-01-10   | 150         | 


                ***Output: 

                | visited_on   | amount       | average_amount |
                |--------------|--------------|----------------|
                | 2019-01-07   | 860          | 122.86         |
                | 2019-01-08   | 840          | 120            |
                | 2019-01-09   | 840          | 120            |
                | 2019-01-10   | 1000         | 142.86         |


                Explanation: 
                1st moving average from 2019-01-01 to 2019-01-07 has an average_amount of (100  110  120  130  110  140  150)/7 = 122.86
                2nd moving average from 2019-01-02 to 2019-01-08 has an average_amount of (110  120  130  110  140  150  80)/7 = 120
                3rd moving average from 2019-01-03 to 2019-01-09 has an average_amount of (120  130  110  140  150  80  110)/7 = 120
                4th moving average from 2019-01-04 to 2019-01-10 has an average_amount of (130  110  140  150  80  110  130  150)/7 = 142.86

               ``` sql
                CREATE or replace TABLE Customer (
                customer_id INT,
                name VARCHAR(50),
                visited_on DATE,
                amount INT
                ) AS (
                SELECT * FROM VALUES
                (1, 'Jhon', '2019-01-01', 100),
                (2, 'Daniel', '2019-01-02', 110),
                (3, 'Jade', '2019-01-03', 120),
                (4, 'Khaled', '2019-01-04', 130),
                (5, 'Winston', '2019-01-05', 110),
                (6, 'Elvis', '2019-01-06', 140),
                (7, 'Anna', '2019-01-07', 150),
                (8, 'Maria', '2019-01-08', 80),
                (9, 'Jaze', '2019-01-09', 110),
                (1, 'Jhon', '2019-01-10', 130),
                (3, 'Jade', '2019-01-10', 150)
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
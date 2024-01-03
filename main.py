import streamlit as st
import snowflake.connector
from pathlib import Path
import time
import pandas as pd
from st_pages import Page, add_page_title, show_pages


st.set_page_config(
  page_title="PinkEnducationInsigts",
  page_icon=":house:",
  layout="wide",
  initial_sidebar_state="expanded",
) 

show_pages(
          [
              Page("main.py", "START FORM HERE", "🏠"),
              Page("pages/dropout_rate.py", "Female Drop out rates", "1️⃣"),
            #   Page("pages/Leet1321.py", "Restaurant Growth", "2️⃣"),
            #   Page("pages/Leet185.py", "Department Top Three Salaries", "3️⃣"),
          ]
      )  
# add_page_title()
# Function to store Snowflake credentials in session state
def store_credentials(account, role, warehouse, database, schema, user, password):
    st.session_state.account = account
    st.session_state.role = role
    st.session_state.warehouse = warehouse
    st.session_state.database = database
    st.session_state.schema = schema
    st.session_state.user = user
    st.session_state.password = password

# Function to connect to Snowflake
# @st.cache    
# Create a Snowflake connection function
   
def create_snowflake_connection(account, role, warehouse, database, schema, user, password):
    try:
        conn = snowflake.connector.connect(
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            user=user,
            password=password,
            client_session_keep_alive=True
        )
        st.toast("Connection to Snowflake successfully!", icon='🎉')
        time.sleep(.5)
        st.balloons()
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")    
    # return conn



# def execute_query(query):
#     try:
#         conn = snowflake.connector.connect(
#             account=st.session_state.account,
#             role=st.session_state.role,
#             warehouse=st.session_state.warehouse,
#             database=st.session_state.database,
#             schema=st.session_state.schema,
#             user=st.session_state.user,
#             password=st.session_state.password,
#             client_session_keep_alive=True
#         )
#         cursor = conn.cursor()
#         cursor.execute(query)
#         result = cursor.fetchall()
#         conn.close()
#         return result
#     except Exception as e:
#         st.error(f"Error executing query: {str(e)}")
#         return None
with st.sidebar:

    st.markdown("[![Foo](https://cdn2.iconfinder.com/data/icons/social-media-2285/512/1_Linkedin_unofficial_colored_svg-48.png)](https://www.linkedin.com/in/danammahiremath/) Connect me.")
    st.sidebar.header("Snowflake Credentials")
    expander = st.expander("Set Up SF Connection")
    account = expander.text_input('Acount','qa07240.ap-southeast-1')
    role = expander.text_input('Role','ACCOUNTADMIN')
    warehouse = expander.text_input('Warehouse','COMPUTE_WH')
    database = expander.text_input('Database','IND_DATA')
    schema = expander.text_input('Schema','IND_SCHEMA')
    user = expander.text_input('User','Hackathon')
    password = expander.text_input("Password", type="password")
    if expander.button("Connect"):
        store_credentials(account, role, warehouse, database, schema, user, password)
        connection = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
# add_page_title()

# show_pages_from_config()









def main(): 
    st.title('How to this app')

if __name__ == "__main__":
    main()

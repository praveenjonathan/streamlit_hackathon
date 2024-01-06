import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import time
import pandas as pd
from st_pages import Page, add_page_title, show_pages


st.set_page_config(
  page_title="INDIAN-FEMALE-EDUCATION-INSIGHTS",
  page_icon=":house:",
  layout="wide",
  initial_sidebar_state="expanded",
) 

show_pages(
          [
              Page("main.py", "START FORM HERE", "🏠"),
              Page("pages/dropout_rate.py", "FEMALE DROP OUT ANALYSIS", "1️⃣"),
              Page("pages/enrl_to_schools.py", "CLASSWISE & AGEWISE ENROLLMENT TO SCHOOLS", "2️⃣"),
              Page("pages/infra_stat.py", "SCHOOL'S INFRA STATISTICS", "3️⃣"),
			  Page("pages/aishe.py", "ALL INDIA SURVEY ON HIGHER EDUCATION", "4️⃣"),
              Page("pages/girls_per_hundered.py", "CLASS-WISE GIRLS PER HUNDRED BOYS", "5️⃣"),
			  Page("pages/gender_parity.py", "GENDER PARITY INDEX IN HIGHER EDUCATION", "6️⃣"),
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
    return conn



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
                st.title('Detail steps how analysis done')

                st.markdown('STEP 1:DATA DOWNLOADED FROM data.gov.in IN CSV FORMAT')

                st.markdown('STEP 2:DATA DOWNLOADED LOADED ALL TABLES WITH MY OWN CREATED UTILITY')
                st.markdown('Upload file to Snowflake')
                file = st.file_uploader('Upload file', type=['xls', 'xlsx', 'csv', 'txt'])

                if file is not None:
                    # Read the file
                    file_extension = file.name.split('.')[-1]
                    if file_extension.lower() in ['xls', 'xlsx', 'csv', 'txt']:
                        data = pd.read_excel(file) if file_extension.lower() in ['xls', 'xlsx'] else pd.read_csv(file, encoding='latin-1')

                        st.subheader('Preview of Uploaded Data')
                        st.write(data.head())

                        # Save data to Snowflake
                        conn = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                        if conn:
                            st.info('Connected to Snowflake!')

                            table_name = st.text_input('Enter table name in Snowflake')

                            if st.button('Save to Snowflake'):
                                try:
                                    data_f=pd.DataFrame(data)
                                    success, nchunks, nrows, _ = write_pandas(conn=conn,df=data_f,table_name=table_name,database=database,schema=schema,auto_create_table=True)
                                    
                                    st.success(f'Dataloaded to snowflake table: {table_name}  rows : {nrows}')
                                except Exception as e:
                                    st.error(f'Error: {str(e)}')
                        else:
                            st.error('Unable to connect to Snowflake. Please check your credentials.')
                st.markdown('STEP 3:COMPLEX SQL QUERUIES CREATED TO ANALYSE DATA SETS')

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

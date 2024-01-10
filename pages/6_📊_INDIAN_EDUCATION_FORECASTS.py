import streamlit as st
import snowflake.connector
import pandas as pd
from st_pages import Page, add_page_title, show_pages
import plotly.express as px
import altair as alt
import numpy as np
import altair as alt
import matplotlib.pyplot as plt
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

# Streamlit app layout
def main():

    st.title("📊 DROP OUT RATES FUTURE PREDICTION IN INDIA USING SNOWFLAKE.ML.FORECAST")
    col1,col2=st.columns(2)
    with col1:
        yr_options = [10,15,20]  
        yr = st.selectbox('Select predicts years:', options=yr_options, index=1)
    with col2:
        s_col_options = ["I_V_BOYS",
                        "I_V_GIRLS",
                        "I_V_TOTAL",
                        "I_VIII_BOYS",
                        "I_VIII_GIRLS",
                        "I_VIII_TOTAL",
                        "I_X_BOYS",
                        "I_X_GIRLS",
                        "I_X_TOTAL",
                        "SC_I_V_BOYS",
                        "SC_I_V_GIRLS",
                        "SC_I_V_TOTAL",
                        "SC_I_VIII_BOYS",
                        "SC_I_VIII_GIRLS",
                        "SC_I_VIII_TOTAL",
                        "SC_I_X_BOYS",
                        "SC_I_X_GIRLS",
                        "SC_I_X_TOTAL",
                        "ST_I_V_BOYS",
                        "ST_I_V_GIRLS",
                        "ST_I_V_TOTAL",
                        "ST_I_VIII_BOYS",
                        "ST_I_VIII_GIRLS",
                        "ST_I_VIII_TOTAL",
                        "ST_I_X_BOYS",
                        "ST_I_X_GIRLS",
                        "ST_I_X_TOTAL"] 
        s_col_index = s_col_options.index("SC_I_X_TOTAL")  # Find the index of the default value
        s_col = st.selectbox('Select class:', options=s_col_options, index=s_col_index)

    Q1=f'''
        BEGIN

        CREATE OR REPLACE VIEW V01_USE_DATA_DROPDATE_FORECAST_{s_col}
            AS
        SELECT TS_FOR_FORECAST,{s_col} FROM V01_DATA_FOR_DRR_FORECAST
        WHERE YEAR(TS_FOR_FORECAST) IN (2008,2009,2010);

        DROP SNOWFLAKE.ML.FORECAST IF EXISTS  DROPDATE_FORECAST_{s_col};
        
        CREATE SNOWFLAKE.ML.FORECAST DROPDATE_FORECAST_{s_col}(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V01_USE_DATA_DROPDATE_FORECAST_{s_col}'),
        TIMESTAMP_COLNAME => 'TS_FOR_FORECAST',
        TARGET_COLNAME => '{s_col}');

        
        CALL DROPDATE_FORECAST!FORECAST(FORECASTING_PERIODS => 365 * {yr} );
        
        LET x := SQLID;
        CREATE OR REPLACE TABLE SF_FORECAST_{s_col} AS 
        SELECT YEAR(TS) YEAR,avg(ROUND(FORECAST,2)) FORECAST,avg(ROUND(LOWER_BOUND,2)) LOWER_BOUND,
        avg(ROUND(UPPER_BOUND,2)) UPPER_BOUND  
        FROM TABLE(RESULT_SCAN(:x))
        GROUP BY YEAR;

        CREATE OR REPLACE VIEW SF_ACTUAL_FORECAST_{s_col}
            AS 
        SELECT YEAR,ROUND(FORECAST,2) ACTUAL_FORECAST,ROUND(LOWER_BOUND,2) LOWER_BOUND,
        ROUND(UPPER_BOUND,2) UPPER_BOUND   FROM SF_FORECAST_{s_col}
        
        UNION ALL
        
        SELECT DISTINCT CAST(YEAR AS INTEGER) YEAR,{s_col},NULL,NULL FROM V01_DATA_FOR_DRR_FORECAST; 
        RETURN 'SF_ACTUAL_FORECAST_{s_col} will have Actual and forecast data';
        END;
        '''
    # Once Generate Forecast clicked Q1, Q2 will be executed 
    if st.button('Generate Forecast'):
            try:
                R1 = execute_query(Q1)
                r1_expander = st.expander(f'Query generated for for executed for class  {s_col} ')
                r1_expander.write(Q1)
                r1_expander.write(R1)

                LOAD_Q=f'''call system$send_email(
                                            'SF_Email_Notifications',
                                            'danuhiremath123@gmail.com',
                                            '[SF_Email_Notifications]:Email Alert: Forecast for dropout rate  class: {s_col}  generated for  {yr}.',
                                            'Forecast for dropout rate  class: {s_col}  generated for  {yr}   .\n   ON:' || TO_VARCHAR(CURRENT_TIMESTAMP()) || 
                                            'Hurryyyy!! :) '
                                             )'''
                execute_query(LOAD_Q)
                st.success(f'Forecast for dropout rate  class: {s_col}  generated for  {yr} Years also email sent')
                Q2=f''' SELECT * FROM SF_ACTUAL_FORECAST_{s_col} '''
                R2 = execute_query(Q2)
                R2_DF = pd.DataFrame(R2)
                R2_DF.index = R2_DF.index + 1
                r2_expander = st.expander(f'Atual and forcast drop out rate generated for {s_col} Years ')
                r2_expander.write(R2_DF)
            except Exception as e:
                    st.error(f'Error: {str(e)}')

            title=f'''Actual and Forecast dropout rate for  class: {s_col}  for  {yr} Years. After 2010 all are predicted'''
            def plot_chart(data, x_axis, y_axes):
                selected_columns = [x_axis] + y_axes
                melted_data = data.melt('YEAR', value_vars=y_axes, var_name='CATEGORY')
                
                chart = alt.Chart(melted_data).mark_line(point=True).encode(
                    x=alt.X('YEAR:N', title=title),
                    y=alt.Y('value:Q', title='DROPOUT_RATE'),
                    color='CATEGORY:N',
                    tooltip=['YEAR:N', alt.Tooltip('value:Q', title='DROPOUT_RATE', format='.2f'), 'CATEGORY:N']
                ).properties(
                    width=600,
                    height=400
                ).configure_legend(
                orient='left',
                title=None,
                labelFontSize=9
                ).configure_axis(grid=False).interactive()
                return chart

            x_axis_column ='YEAR'
            y_axis_columns = st.multiselect('Select Y-axis', options=list(R2_DF.columns)[1:], default=list(R2_DF.columns)[1:])#[list(R2_DF.columns)[1]])

            # Plotting the chart with multiple Y-axis columns
            st.altair_chart(plot_chart(R2_DF, x_axis_column, y_axis_columns), use_container_width=True)


if __name__ == '__main__':
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

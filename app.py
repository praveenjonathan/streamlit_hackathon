import streamlit as st
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Data
data = {
    'State': [
        'Andaman and Nicobar Islands', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chandigarh',
        'Chhattisgarh', 'Dadra & Nagar Haveli', 'Daman & Diu', 'Delhi', 'Goa', 'Gujarat', 'Haryana',
        'Himachal Pradesh', 'Jammu and Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Lakshadweep',
        'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha',
        'Pondicherry', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Tripura', 'Uttar Pradesh',
        'Uttaranchal', 'West Bengal','Telangana','Ladakh'
    ],
    'Dropout_Rate': [
        95.88, 96.62, 129.12, 111.77, 95.03, 88.42, 104.06, 89.5, 87.8, 108.78, 104.97, 100.32, 96.62, 99.8,
        84.03, 109.57, 101.18, 95.68, 83.42, 111.85, 99.93, 145.68, 132.89, 127.88, 116.66, 107.15, 89.53,
        104.33, 102.35, 128.15, 102.4, 112.7, 93.34, 99.98, 103.16,100,100
    ]
}

df = pd.DataFrame(data)

# Read shapefile into GeoDataFrame
india_states_shp = 'https://github.com/Princenihith/Maps_with_python/raw/master/india-polygon.shp'  # Replace with the path to your shapefile
india_states = gpd.read_file(india_states_shp)
st.write(india_states)
india_states.rename(columns={'st_nm': 'State'}, inplace=True)
# Merge dropout rates with GeoDataFrame
merged_data = india_states.merge(df, how='left', on='State')
st.write(merged_data)
# Streamlit app
st.title('Indian Primary School Dropout Rates')

# Display the data
st.write(df)

# Plotting the map
fig, ax = plt.subplots(1, 1, figsize=(10, 10))
merged_data.plot(column='Dropout_Rate', cmap='YlOrRd', linewidth=0.8, ax=ax, edgecolor='0.8', legend=True)
ax.axis('off')

# Display the map in Streamlit
st.pyplot(fig)
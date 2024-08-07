import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
import os
from snowflake.snowpark.files import SnowflakeFile

import pyarrow.parquet as pq
import pandas as pd
import streamlit as st

from common.utils import *
from snowflake.snowpark.context import get_active_session

session = get_active_session()

GET_METADATA = "SNOWPARK_TESTDB.PUBLIC.GET_METADATA"

        
# Function to apply styles with Excel-like colors for right (green) and warning (yellow)
def highlight_conditions(val):
    if isinstance(val, bool):
        if val:
            return 'background-color: #C6EFCE; color: #006100;'  # Light green background with dark green text
        else:
            return 'background-color: #FFEB9C; color: #9C6500;'  # Light yellow background with dark yellow text
    return ''





st.title(f"Parquet Explorer")


with st.sidebar:
    st.header("Configuration")
    st.caption(session.get_current_account())
    st.caption(session.get_current_database())
    current_database = st.selectbox("Select Database", get_databases())

    if current_database:
        current_schema = st.selectbox("Select Schema", get_schemas(current_database))
    
    if current_schema:
        df_external_tables = get_external_tables(current_database, current_schema)

        if len(df_external_tables) > 0:
            st.success(f" {len(df_external_tables)} found")
        else:
            st.warning("No external tables found")

previous_cols = df_external_tables.columns

df_external_tables.insert(0, "PROCESS", False)
selected = st.data_editor (df_external_tables, disabled=previous_cols)


selected_count = len(selected[selected["PROCESS"] == True])
if selected_count==0:
    st.stop()

if selected_count>1:
    st.error("Please select only one table")
    st.stop()


for idx, row in selected.iterrows():
    if row['PROCESS'] == True:
        stage = row['STAGE']
        location = str(row['LOCATION'])
        try:
            files = session.sql(f"LIST {stage}").select(col('"name"'),col('"size"')).collect()
        except:
            files = []
        
        files = [(x[0].replace(location,''),x[1]) for x in files]
        selected_file = st.selectbox("select a file",files, format_func=lambda x:x[0])
        if selected_file and st.button("check_file"):
            selected_size = selected_file[1]
            selected_file = selected_file[0]
            selected_file = stage + selected_file
            st.write("Checking: " + selected_file)
            metadata = session.call(GET_METADATA,selected_file)
            import json
            metadata = json.loads(metadata)
            st.write(metadata)
            column_data = []
            total_row_groups = 0
            row_groups_sizes = []
            _2MB = 2 * 1024 * 1024
            _1GB = 1 * 1024 * 1024 * 1024
            for i,c in enumerate(metadata["row_groups"]):
                total_row_groups = total_row_groups + 1
                total_uncompressed_size = 0
                total_compressed_size = 0
                for col in c["columns"].keys():
                    col_data = c["columns"][col]
                    total_uncompressed_size = col_data["total_uncompressed_size"]
                    total_compressed_size = col_data["total_compressed_size"]
                    column_data.append({
                        "row_group_index" : i,
                        "col": col,
                        "nvd": col_data["distinct_count"],
                        "min_max": col_data["min_max"],
                        "null_count":col_data["null_count"],
                        "compressed_size":col_data["total_compressed_size"],
                        "uncompressed_size":col_data["total_uncompressed_size"],
                    })
                row_groups_sizes.append({"index":i,"compressed":total_compressed_size, "uncompressed":total_uncompressed_size})
            
            df = pd.DataFrame(column_data)
            styled_df = df.style.applymap(highlight_conditions, subset=['nvd', 'min_max', 'null_count'])

            st.write(styled_df)
            if total_row_groups == 1:
                st.success(f"row groups count: {total_row_groups}")
            else:
                st.warning(f"row groups count: {total_row_groups}")
            if selected_size < _2MB:
                st.warning(f"Small files {selected_size} < 2GB lead to latency overheards")
            elif selected_size > _1GB:
                st.warning(f"Large files {selected_size} > 1GB can lead to OOM")
            else:
                st.success(f"File size: {selected_size}")
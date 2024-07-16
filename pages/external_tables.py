import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, contains
import os
import json

from common.utils import *
from snowflake.snowpark.context import get_active_session
import streamlit as st

session = get_active_session()

st.title(f"Upgrading External Tables")

df_external_tables = None
with st.sidebar:
    st.header("Configuration")
    st.caption(session.get_current_account())
    st.caption(session.get_current_database())
    current_database = st.selectbox("Select Database", get_databases())

    if current_database:
        current_schema = st.selectbox("Select Schema", get_schemas(current_database))
    
    if current_schema:
        df_external_tables = get_external_tables(current_database, current_schema)


if df_external_tables is not None and len(df_external_tables) > 0:
    st.success(f" {len(df_external_tables)} found")
else:
    st.warning("No external tables found")
    st.stop()

previous_cols = df_external_tables.columns

df_external_tables.insert(0, "PROCESS", False)
selected = st.data_editor (df_external_tables, disabled=previous_cols)


selected_count = len(selected[selected["PROCESS"] == True])
if selected_count==0:
    st.stop()

delta = False

for idx, row in selected.iterrows():
    #st.write(row)
    if row["PROCESS"] == True:
        if "DELTA" == row["TABLE_FORMAT"]:
            delta = True
            break

if delta:
    catalog_name = st.text_input("catalog_name","delta_direct_catalog")
    create_catalog_sql = f"""
CREATE OR REPLACE CATALOG INTEGRATION {catalog_name}
  CATALOG_SOURCE=OBJECT_STORE
  TABLE_FORMAT=DELTA
  ENABLED=TRUE;"""
else:
    catalog_name = st.text_input("catalog_name","parquet_direct_catalog")
    create_catalog_sql = f"""
CREATE OR REPLACE CATALOG INTEGRATION {catalog_name}
  CATALOG_SOURCE=OBJECT_STORE
  TABLE_FORMAT=NONE
  ENABLED=TRUE;"""

st.code(create_catalog_sql,"sql")
if st.button("CREATE CATALOG"):
    session.sql(create_catalog_sql).count()
    st.success(f"Catalog {catalog_name} created")
    

for idx, row in selected.iterrows():
    if row["PROCESS"] == True:
        full_name = row['TABLE_CATALOG'] + "." + row['TABLE_SCHEMA'] + "." + row['TABLE_NAME']
        with st.expander(full_name):
            stage = row["STAGE"][1:]
            df_stage_props = session.sql(f"describe stage {stage}")
            df2 = df_stage_props[df_stage_props['"property"']==lit("AWS_ROLE")]
            AWS_ROLE_ARN       = df_stage_props[df_stage_props['"property"']==lit("AWS_ROLE")].first()[3]
            AWS_EXTERNAL_ID    = df_stage_props[df_stage_props['"property"']==lit("AWS_EXTERNAL_ID")].first()[3]
            SNOWFLAKE_IAM_USER = df_stage_props[df_stage_props['"property"']==lit("SNOWFLAKE_IAM_USER")].first()[3]
            URL                = json.loads(df_stage_props[df_stage_props['"property"']==lit("URL")].first()[3])[0]
            if URL.startswith("s3"):
                ext_volume_name  = st.text_input("volume name:",f"EXT_VOL_{row['TABLE_NAME']}", key=f"volume_name{idx}")
                ext_volume_code = f"""
        CREATE OR REPLACE EXTERNAL VOLUME {ext_volume_name}
           STORAGE_LOCATIONS =
              (
                 (
                    NAME = '{ext_volume_name}'
                    STORAGE_PROVIDER = 'S3'
                    STORAGE_BASE_URL = '{URL}'
                    STORAGE_AWS_ROLE_ARN = '{AWS_ROLE_ARN}'
                    STORAGE_AWS_EXTERNAL_ID = '{AWS_EXTERNAL_ID}'
                 )
              );
        """
                if st.button("CREATE VOLUME",key=f"createvolume{idx}"):
                    session.sql(ext_volume_code).count()
                    st.success(f"Volume {ext_volume_name} was created")
                
                icebert_table = f"""
        CREATE OR REPLACE ICEBERG TABLE {full_name}_EXT 
          CATALOG = {catalog_name}
          EXTERNAL_VOLUME = {ext_volume_name}
          BASE_LOCATION=''"""
                if delta:
                    icebert_table += ";"
                else:
                    icebert_table += " INFER_SCHEMA = TRUE; "

                st.code(ext_volume_code,"sql")
                st.code(icebert_table,"sql")
                if st.button("CREATE TABLE",key=f"createtable{idx}"):
                    session.sql(icebert_table).count()
                    st.success(f"Table {full_name}_EXT was created")
                if st.button("TEST TABLE",key=f"testtable{idx}"):
                    st.write(session.sql(f"select * from {full_name}_EXT "))
                if st.button("DROP",key=f"droptable{idx}"):
                        session.sql(f"DROP TABLE {full_name}_EXT").count()
                        st.success(f"TABLE {ext_volume_name}_EXT dropped")
            else:
                st.warning("Only S3 implemented for now")
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, contains
import os
import json

def get_catalogs():    
    df = get_connection().sql("show integrations")
    return [x[0] for x in df.filter(col('"type"')==lit("CATALOG")).select(col('"name"')).collect()]



def get_connection():
    return Session.builder.config("connection_name","migrations1").getOrCreate()

@st.cache_data
def get_databases():
    session = get_connection()
    #query = "select distinct TABLE_CATALOG from snowflake.account_usage.tables where TABLE_TYPE = 'EXTERNAL TABLE'"
    return [x[0] for x in session.table("INFORMATION_SCHEMA.DATABASES").select("DATABASE_NAME").collect()]
   # return [x[0] for x in session.sql(query).collect()]

@st.cache_data
def get_schemas(database_name):
    session = get_connection()
    return ["(all)"] + [x[0] for x in session.table("INFORMATION_SCHEMA.SCHEMATA").where(col("CATALOG_NAME")==lit(database_name)).select("SCHEMA_NAME").collect()]

@st.cache_data
def get_external_tables(database_name,schema_name):
    session = get_connection()
    try:
        if schema_name == "(all)":
            session.sql(f"show external tables in DATABASE \"{database_name}\" ").write.mode("overwrite").save_as_table("_external_tables")
        else:
            session.sql(f"show external tables in DATABASE \"{database_name}\" SCHEMA \"{schema_name}\" ").write.mode("overwrite").save_as_table("_external_tables")    
        #st.write(session.table("_external_tables"))
        return session.sql(f"""
    with 
    external_tables_info as (
        select "name" TABLE_NAME, "database_name" TABLE_CATALOG, "schema_name" TABLE_SCHEMA, "location" LOCATION ,"file_format_name",
        SPLIT("file_format_name",'.') format_parts,
        case array_size(format_parts)
          when 3 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
          when 2 then upper(TABLE_CATALOG || '.' || "file_format_name"                       ) 
          when 1 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
        end FILE_FORMAT_NAME,
        "file_format_type",
        "stage" STAGE
        from _external_tables et where "invalid" <> true
    ),
    file_formats as (
    select upper(FILE_FORMAT_CATALOG || '.' || FILE_FORMAT_SCHEMA || '.' || FILE_FORMAT_NAME) FILE_FORMAT_NAME,FILE_FORMAT_TYPE  
    from {database_name}.information_schema.file_formats
    )
    
    select 
        INFO.TABLE_CATALOG, 
        info.TABLE_SCHEMA, 
        info.TABLE_NAME, 
        COALESCE(info."file_format_type",ff.FILE_FORMAT_TYPE) FILE_FORMAT_TYPE, 
        info.LOCATION,
        info.FILE_FORMAT_NAME,
        info.STAGE
    from external_tables_info info
    left join file_formats ff on
    info.FILE_FORMAT_NAME = ff.FILE_FORMAT_NAME
    where FILE_FORMAT_TYPE='PARQUET'
        """).to_pandas()
    except Exception as ex:
        st.error(ex.message)
        

import streamlit as st


st.title(f"Upgrading External Tables")

df_external_tables = None
with st.sidebar:
    st.header("Configuration")
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



catalog_name = st.text_input("catalog_name","parquet_direct_catalog")

create_catalog_sql = f"""
CREATE OR REPLACE CATALOG INTEGRATION {catalog_name}
  CATALOG_SOURCE=OBJECT_STORE
  TABLE_FORMAT=NONE
  ENABLED=TRUE;"""

st.code(create_catalog_sql,"sql")
if st.button("CREATE CATALOG"):
    get_connection().sql(create_catalog_sql).count()
    st.success(f"Catalog {catalog_name} created")
    



for idx, row in selected.iterrows():
    if row["PROCESS"] == True:
        full_name = row['TABLE_CATALOG'] + "." + row['TABLE_SCHEMA'] + "." + row['TABLE_NAME']
        with st.expander(full_name):
            stage = row["STAGE"][1:]
            df_stage_props = get_connection().sql(f"describe stage {stage}")
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
                    get_connection().sql(ext_volume_code).count()
                    st.success(f"Volume {ext_volume_name} was created")
                
                icebert_table = f"""
        CREATE OR REPLACE ICEBERG TABLE {full_name}_EXT 
          CATALOG = {catalog_name}
          EXTERNAL_VOLUME = {ext_volume_name}
          BASE_LOCATION=''
          INFER_SCHEMA = TRUE
          ;
        """
                st.code(ext_volume_code,"sql")
                st.code(icebert_table,"sql")
                if st.button("CREATE TABLE",key=f"createtable{idx}"):
                    get_connection().sql(icebert_table).count()
                    st.success(f"Table {full_name}_EXT was created")
                if st.button("TEST TABLE",key=f"testtable{idx}"):
                    st.write(get_connection().sql(f"select * from {full_name}_EXT "))
                if st.button("DROP",key=f"droptable{idx}"):
                        get_connection().sql(f"DROP TABLE {full_name}_EXT").count()
                        st.success(f"TABLE {ext_volume_name}_EXT dropped")
            else:
                st.warning("Only S3 implemented for now")
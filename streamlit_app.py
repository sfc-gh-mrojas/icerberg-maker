import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit


def get_catalogs():    
    df = get_connection().sql("show integrations")
    return [x[0] for x in df.filter(col('"type"')==lit("CATALOG")).select(col('"name"')).collect()]



def get_connection():
    return Session.builder.getOrCreate()
    
@st.cache_data
def get_databases():
    session = get_connection()
    return [x[0] for x in session.table("INFORMATION_SCHEMA.DATABASES").select("DATABASE_NAME").collect()]

@st.cache_data
def get_schemas(database_name):
    session = get_connection()
    return ["(all)"] + [x[0] for x in session.table("INFORMATION_SCHEMA.SCHEMATA").where(col("CATALOG_NAME")==lit(database_name)).select("SCHEMA_NAME").collect()]

@st.cache_data
def get_external_tables(database_name,schema_name):
    session = get_connection()
    if schema_name == "(all)":
        session.sql(f"show external tables in DATABASE \"{database_name}\" ").write.mode("overwrite").save_as_table("_external_tables")
    else:
        session.sql(f"show external tables in DATABASE \"{database_name}\" SCHEMA \"{schema_name}\" ").write.mode("overwrite").save_as_table("_external_tables")
    return session.sql("""
with 
external_tables_info as (
    select "name" TABLE_NAME, "database_name" TABLE_CATALOG, "schema_name" TABLE_SCHEMA, "location" LOCATION ,"file_format_name",
    SPLIT("file_format_name",'.') format_parts,
    case array_size(format_parts)
      when 3 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
      when 2 then upper(TABLE_CATALOG || '.' || "file_format_name"                       ) 
      when 1 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
    end FILE_FORMAT_NAME
    from _external_tables et
),
file_formats as (
select upper(FILE_FORMAT_CATALOG || '.' || FILE_FORMAT_SCHEMA || '.' || FILE_FORMAT_NAME) FILE_FORMAT_NAME,FILE_FORMAT_TYPE  from information_schema.file_formats
)
select INFO.TABLE_CATALOG, info.TABLE_SCHEMA, info.TABLE_NAME, ff.FILE_FORMAT_TYPE, info.LOCATION 
from external_tables_info info
left join file_formats ff on
info.FILE_FORMAT_NAME = ff.FILE_FORMAT_NAME;
    """).to_pandas()
        

import streamlit as st


st.title(f"Upgrading External Tables")


with st.sidebar:
    st.header("Configuration")
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

catalog_name = st.text_input("catalog_name","parquet_direct_catalog")

create_catalog_sql = f"""
CREATE OR REPLACE CATALOG INTEGRATION {catalog_name}
  CATALOG_SOURCE=OBJECT_STORE
  TABLE_FORMAT=NONE
  ENABLED=TRUE;"""

st.code(create_catalog_sql,"sql")
if st.button("CREATE CATALOG"):
    get_connection.sql(create_catalog_sql).count()
    st.success(f"Catalog {catalog_name} created")
    






for idx, row in selected.iterrows():
    if row["PROCESS"] == True:  
        full_name = row['TABLE_CATALOG'] + "." + row['TABLE_SCHEMA'] + "." + row['TABLE_NAME']
        
        with st.expander(full_name,False):
            volume_name = st.text_input("volume name:",f"external_volume{idx+1}", key=f"volume_name{idx}")
            location_name= st.text_input("location_name", row['TABLE_NAME'], key=f"location_name_{idx}")
            if st.checkbox("create external volume",key=f"volume{idx}"):
                if "s3" in row["LOCATION"]:
                    st.markdown("[Guide setup S3](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3)")
                    base_url        = st.text_input("base_url",row['LOCATION'], key=f"baseurl_{idx}")
                    AWS_ROLE_ARN    = st.text_input("role_arn",'',key=f"rolearn_{idx}")
                    AWS_EXTERNAL_ID = st.text_input("external_id",row['TABLE_NAME'].lower() + "_external_id",key=f"externalid_{idx}")
                    st.code(f"""
    CREATE OR REPLACE EXTERNAL VOLUME {volume_name}
       STORAGE_LOCATIONS =
          (
             (
                NAME = '{location_name}'
                STORAGE_PROVIDER = 'S3'
                STORAGE_BASE_URL = '{base_url}'
                STORAGE_AWS_ROLE_ARN = '{AWS_ROLE_ARN}'
                STORAGE_AWS_EXTERNAL_ID = '{AWS_EXTERNAL_ID}'
             )
          );""")
                if "azure" in row["LOCATION"]:
                    st.markdown("[Guide setup Azure](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-azure)")
                    base_url        = st.text_input("base_url",row['LOCATION'], key=f"baseurl_{idx}")
                    AZURE_TENANT_ID    = st.text_input("azure_tenant_id",'',key=f"tenantid_{idx}")
                    st.code(f"""
    CREATE OR REPLACE EXTERNAL VOLUME {volume_name}
       STORAGE_LOCATIONS =
          (
             (
                NAME = '{location_name}'
                STORAGE_PROVIDER = 'AZURE'
                STORAGE_BASE_URL = '{base_url}'
                AZURE_TENANT_ID = '{AZURE_TENANT_ID}'
             )
          );""")
                if "gcs" in row["LOCATION"]:
                    st.markdown("[Guide setup GCS](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-gcs)")
                    base_url        = st.text_input("base_url",row['LOCATION'], key=f"baseurl_{idx}")
                    GCS_ENCRIPTION  = st.selectbox("gcs encription",["GCS_SSE_KMS","NONE"],key=f"gcs_encription_{idx}")
                    GCS_KEY_ID      = st.text_input('key id','',key=f'key_id_{idx}')
                    st.code(f"""
    CREATE OR REPLACE EXTERNAL VOLUME {volume_name}
       STORAGE_LOCATIONS =
          (
             (
                NAME = '{location_name}'
                STORAGE_PROVIDER = 'GCS'
                STORAGE_BASE_URL = '{base_url}'
                ENCRYPTION=(TYPE='{GCS_ENCRIPTION}' KMS_KEY_ID = '{GCS_KEY_ID}')
             )
          );""")
            table_sql = f""" 
CREATE ICEBERG TABLE {full_name} 
  CATALOG = usecase1_catalog
  EXTERNAL_VOLUME = {volume_name}
  BASE_LOCATION='/'
  INFER_SCHEMA = TRUE
  ;
             """
            st.code(table_sql,"sql")
            if st.button("CREATE TABLE"):
                get_connection().sql(table_sql).count()


        

    
    
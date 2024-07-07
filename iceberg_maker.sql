CREATE OR REPLACE PROCEDURE SNOWPARK_TESTDB.PUBLIC.GET_METADATA("FILEPATH" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pyarrow')
HANDLER = 'display_parquet_metadata'
EXECUTE AS OWNER
AS $$
from snowflake.snowpark.files import SnowflakeFile
import pyarrow.parquet as pq
def display_parquet_metadata(file):
    with SnowflakeFile.open(file, "rb",require_scoped_url=False) as file:
        # Read the Parquet file
        parquet_file = pq.ParquetFile(file)

   # Initialize the metadata dictionary
    metadata_dict = {
        "num_row_groups": parquet_file.metadata.num_row_groups,
        "schema": {},
        "row_groups": []
    }

    # Extract schema metadata
    schema = parquet_file.metadata.schema
    for idx, name in enumerate(schema.names):
        field = schema.column(idx)
        metadata_dict["schema"][name] = {
            "physical_type": str(field.physical_type),
            "logical_type": str(field.logical_type) if hasattr(field, "logical_type") else None
        }

    # Extract row group metadata
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        row_group = parquet_file.metadata.row_group(row_group_index)
        row_group_info = {
            "total_byte_size": row_group.total_byte_size,
            "num_rows": row_group.num_rows,
            "columns": {}
        }
        for col_index in range(row_group.num_columns):
            column_chunk = row_group.column(col_index)
            # Access column metadata
            meta = column_chunk.statistics
            row_group_info["columns"][column_chunk.path_in_schema] = {
                "file_offset": column_chunk.file_offset,
                "total_compressed_size": column_chunk.total_compressed_size,
                "total_uncompressed_size": column_chunk.total_uncompressed_size,
                "distinct_count": meta.has_distinct_count if meta is not None else False,
                "min_max": meta.has_min_max if meta is not None else False,
                "null_count": meta.has_null_count if meta is not None else False
            }
        metadata_dict["row_groups"].append(row_group_info)

    return metadata_dict
$$;


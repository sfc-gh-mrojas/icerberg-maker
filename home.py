import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, contains
import os
import json

        

import streamlit as st


st.title(f"Upgrading External Tables")
st.write("This app will help you upgrade your tables to the Iceberg format")
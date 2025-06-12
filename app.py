# app.py
import streamlit as st
import google.generativeai as genai
import pandas as pd
import json

# Import configuration and agents
import config
from agents.data_pipeline_agent import DataPipelineAgent
from agents.data_warehouse_agent import DataWarehouseAgent
from agents.data_mart_agent import DataMartAgent
from tools.db_tools import create_table_ddl, execute_sql_query, get_db_engine # Import tools for initial setup

# --- Initialize Agents ---
# Ensure the API key is set before initializing models
genai.configure(api_key=config.GEMINI_API_KEY)
pipeline_agent = DataPipelineAgent(api_key=config.GEMINI_API_KEY)
warehouse_agent = DataWarehouseAgent(api_key=config.GEMINI_API_KEY)
mart_agent = DataMartAgent(api_key=config.GEMINI_API_KEY)

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Agentic AI for Data Management Prototype")

st.sidebar.header("Agent Control Panel")

# Initial Database Setup (Run once or on app start)
if 'db_initialized' not in st.session_state:
    st.session_state.db_initialized = False

with st.sidebar.expander("Database Setup"):
    st.markdown("Run this once to initialize your SQLite database.")
    if st.button("Initialize Prototype Database"):
        try:
            # Create a test customer staging table (e.g., from a CSV)
            customer_schema = {"customer_id": "INTEGER PRIMARY KEY", "name": "TEXT", "email": "TEXT", "registration_date": "TEXT", "is_active": "BOOLEAN"}
            create_table_ddl(config.DATABASE_URL, "stg_customers", json.dumps(customer_schema))

            # Create a test sales staging table
            sales_schema = {"order_id": "INTEGER PRIMARY KEY", "customer_id": "INTEGER", "product_id": "TEXT", "order_date": "TEXT", "amount": "REAL"}
            create_table_ddl(config.DATABASE_URL, "stg_sales", json.dumps(sales_schema))

            # Create a dummy fact_sales and dim_customer for DW
            fact_sales_schema = {"sale_id": "INTEGER PRIMARY KEY AUTOINCREMENT", "order_id": "INTEGER", "customer_key": "INTEGER", "product_id": "TEXT", "sale_date": "DATE", "amount": "REAL"}
            create_table_ddl(config.DATABASE_URL, "fact_sales", json.dumps(fact_sales_schema))

            dim_customer_schema = {"customer_key": "INTEGER PRIMARY KEY AUTOINCREMENT", "customer_id": "INTEGER", "name": "TEXT", "email": "TEXT", "start_date": "DATE", "end_date": "DATE", "is_current": "BOOLEAN"}
            create_table_ddl(config.DATABASE_URL, "dim_customer", json.dumps(dim_customer_schema))

            st.sidebar.success("Database initialized with `stg_customers`, `stg_sales`, `fact_sales`, `dim_customer` tables.")
            st.session_state.db_initialized = True
        except Exception as e:
            st.sidebar.error(f"Error initializing DB: {e}")

st.sidebar.markdown("---")
agent_choice = st.sidebar.radio(
    "Choose an Agent to interact with:",
    ("Data Pipeline Agent", "Data Warehouse Agent", "Data Mart Agent")
)

st.subheader(f"Interact with {agent_choice}")

user_prompt = st.text_area("Enter your command for the Agent:", height=150, key=f"{agent_choice}_prompt")
execute_button = st.button("Execute Command", key=f"{agent_choice}_execute")

if execute_button and user_prompt:
    st.info("Agent is processing your request...")
    st.markdown("---")
    st.markdown("#### Agent's Thought Process & Tool Calls:")

    if agent_choice == "Data Pipeline Agent":
        full_response, tool_outputs = pipeline_agent.process_prompt(user_prompt)
        st.markdown("#### Final Agent Response:")
        st.markdown(full_response)
        if tool_outputs:
            st.markdown("#### Data Pipeline Actions:")
            for output in tool_outputs:
                st.json(output)
                if output['name'] == 'execute_sql_query' and "SELECT" in output['args'].get('query', '').upper():
                    try:
                        df_result = pd.read_json(output['output'])
                        st.dataframe(df_result)
                    except ValueError:
                        st.warning("Could not display query result as DataFrame (might not be tabular data).")
                elif output['name'] == 'create_dataframe_from_csv_content':
                     try:
                        df_result = pd.read_json(output['output'])
                        st.dataframe(df_result)
                     except ValueError:
                        st.warning("Could not display CSV content as DataFrame.")

    elif agent_choice == "Data Warehouse Agent":
        full_response, generated_sql = warehouse_agent.process_prompt(user_prompt)
        st.markdown("#### Final Agent Response:")
        st.markdown(full_response)
        if generated_sql:
            st.markdown("#### Generated SQL DDL/DML:")
            st.code(generated_sql, language="sql")

    else: # Data Mart Agent
        full_response, generated_sql, query_results_json = mart_agent.process_prompt(user_prompt)
        st.markdown("#### Final Agent Response:")
        st.markdown(full_response)
        if generated_sql:
            st.markdown("#### Generated SQL for Data Mart:")
            st.code(generated_sql, language="sql")
        if query_results_json:
            st.markdown("#### Data Mart Preview:")
            try:
                df_result = pd.read_json(query_results_json)
                st.dataframe(df_result)
            except ValueError:
                st.warning("Could not display Data Mart result as DataFrame.")

st.sidebar.markdown("---")
st.sidebar.markdown("### How to Use:")
st.sidebar.write("1. Set your `GEMINI_API_KEY` in `config.py` or as an environment variable.")
st.sidebar.write("2. Click 'Initialize Prototype Database' to create basic tables.")
st.sidebar.write("3. Select an Agent and enter your command in natural language.")
st.sidebar.write("4. Observe the Agent's thought process and the code/actions it generates.")

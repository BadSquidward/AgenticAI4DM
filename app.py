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

# Import tools for initial database setup (optional, but convenient here)
from tools.db_tools import create_table_ddl, execute_sql_query, get_db_engine

# --- Streamlit UI Page Configuration (MUST BE FIRST) ---
st.set_page_config(layout="wide") # ตั้งค่า Layout ให้กว้าง
st.title("Agentic AI for Data Management Prototype")
# --- END UI Page Configuration ---

# --- DEBUGGING: Display the API Key being used (REMOVE THIS LINE IN PRODUCTION) ---
# WARNING: Exposing API keys directly in the UI is highly insecure for production apps.
# Use this only for temporary debugging and ensure to remove it before public deployment.
#st.sidebar.info(f"DEBUG: API Key being used (first 5 chars): `{config.GEMINI_API_KEY[:5]}...`")
# --- END DEBUGGING SECTION ---

# --- Initialize Agents ---
# กำหนด API Key ก่อนที่จะ Initialise Models
genai.configure(api_key=config.GEMINI_API_KEY)

# สร้าง Instance ของ Agent แต่ละตัว (จะสร้างและจัดการ chat_session ภายในแต่ละ Agent)
# Model ที่ใช้จะถูกกำหนดใน constructor ของแต่ละ Agent
pipeline_agent = DataPipelineAgent(api_key=config.GEMINI_API_KEY)
warehouse_agent = DataWarehouseAgent(api_key=config.GEMINI_API_KEY)
mart_agent = DataMartAgent(api_key=config.GEMINI_API_KEY)


st.sidebar.header("แผงควบคุม Agent")

# ส่วนสำหรับตั้งค่าฐานข้อมูลเริ่มต้น (รันครั้งเดียว)
if 'db_initialized' not in st.session_state:
    st.session_state.db_initialized = False

with st.sidebar.expander("ตั้งค่าฐานข้อมูล Prototype"):
    st.markdown("คลิกปุ่มนี้เพื่อสร้างตารางเริ่มต้นสำหรับฐานข้อมูล SQLite ของคุณ")
    if st.button("เริ่มต้นฐานข้อมูล Prototype"):
        try:
            # สร้างตาราง Staging สำหรับลูกค้า
            customer_schema = {"customer_id": "INTEGER PRIMARY KEY", "name": "TEXT", "email": "TEXT", "registration_date": "TEXT", "is_active": "BOOLEAN"}
            create_table_ddl(config.DATABASE_URL, "stg_customers", json.dumps(customer_schema))
            
            # สร้างตาราง Staging สำหรับยอดขาย
            sales_schema = {"order_id": "INTEGER PRIMARY KEY", "customer_id": "INTEGER", "product_id": "TEXT", "order_date": "TEXT", "amount": "REAL"}
            create_table_ddl(config.DATABASE_URL, "stg_sales", json.dumps(sales_schema))

            # สร้างตาราง Fact (สำหรับ DW)
            fact_sales_schema = {"sale_id": "INTEGER PRIMARY KEY AUTOINCREMENT", "order_id": "INTEGER", "customer_id": "INTEGER", "product_id": "TEXT", "sale_date": "TEXT", "amount": "REAL"}
            create_table_ddl(config.DATABASE_URL, "fact_sales", json.dumps(fact_sales_schema))

            # สร้างตาราง Dimension (สำหรับ DW)
            dim_customer_schema = {"customer_key": "INTEGER PRIMARY KEY AUTOINCREMENT", "customer_id": "INTEGER", "name": "TEXT", "email": "TEXT", "start_date": "TEXT", "end_date": "TEXT", "is_current": "BOOLEAN"}
            create_table_ddl(config.DATABASE_URL, "dim_customer", json.dumps(dim_customer_schema))

            st.sidebar.success("ฐานข้อมูลเริ่มต้นด้วยตาราง `stg_customers`, `stg_sales`, `fact_sales`, `dim_customer` เรียบร้อยแล้ว!")
            st.session_state.db_initialized = True
        except Exception as e:
            st.sidebar.error(f"เกิดข้อผิดพลาดในการเริ่มต้นฐานข้อมูล: {e}")

st.sidebar.markdown("---")
# ตัวเลือก Agent ที่ผู้ใช้จะโต้ตอบด้วย
agent_choice = st.sidebar.radio(
    "เลือก Agent ที่ต้องการโต้ตอบด้วย:",
    ("Data Pipeline Agent", "Data Warehouse Agent", "Data Mart Agent")
)

st.subheader(f"โต้ตอบกับ {agent_choice}")

# --- NEW: Initialize chat history for the selected agent ---
if agent_choice not in st.session_state:
    st.session_state[agent_choice] = [] # Stores messages for each agent
# --- END NEW ---

# --- NEW: Display chat messages from history ---
for message in st.session_state[agent_choice]:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql_code" in message and message["sql_code"]:
            st.code(message["sql_code"], language="sql")
        if "data_preview" in message and message["data_preview"] is not None:
            try:
                # Assuming data_preview is JSON string of DataFrame
                df = pd.read_json(message["data_preview"])
                st.dataframe(df)
            except ValueError:
                st.warning("ไม่สามารถแสดงผลลัพธ์ข้อมูลเป็น DataFrame ได้.")
# --- END NEW ---

# Textarea สำหรับให้ผู้ใช้ป้อนคำสั่ง
user_prompt = st.chat_input("ป้อนคำสั่งของคุณสำหรับ Agent:", key=f"{agent_choice}_prompt_input") # ใช้ st.chat_input
# execute_button = st.button("ประมวลผลคำสั่ง", key=f"{agent_choice}_execute") # ไม่ใช้แล้ว

if user_prompt: # ตรวจสอบเมื่อมีการป้อน prompt
    if not st.session_state.db_initialized:
        st.warning("โปรดเริ่มต้นฐานข้อมูล Prototype ก่อนดำเนินการ")
        # ไม่ต้องเพิ่ม prompt ลง history ถ้า DB ยังไม่ initialized
    else:
        # --- NEW: Add user prompt to chat history ---
        st.session_state[agent_choice].append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)
        # --- END NEW ---

        st.info("Agent กำลังประมวลผลคำสั่งของคุณ... โปรดรอสักครู่")
        # st.markdown("---") # อาจไม่จำเป็นต้องมีเส้นแบ่งใน chat UI
        # st.markdown("#### กระบวนการคิดและการเรียกใช้ Tool ของ Agent:") # จะไปอยู่ในการตอบกลับของ Agent

        # --- NEW: Process prompt and display agent's response ---
        with st.chat_message("agent"):
            agent_response_container = st.empty() # เพื่อให้ข้อความ streaming
            full_response_text = ""
            generated_sql = ""
            query_results_json = None

            if agent_choice == "Data Pipeline Agent":
                full_response_text, tool_outputs = pipeline_agent.process_prompt(user_prompt, agent_response_container)
                # DPA's process_prompt now handles streaming its own output and tool calls
                # And returns the final consolidated text.
                # Data preview from tool_outputs handled inside the agent's process_prompt.
            elif agent_choice == "Data Warehouse Agent":
                full_response_text, generated_sql = warehouse_agent.process_prompt(user_prompt, agent_response_container)
            else: # Data Mart Agent
                full_response_text, generated_sql, query_results_json = mart_agent.process_prompt(user_prompt, agent_response_container)

            # --- NEW: Append agent's final response to chat history ---
            agent_message_content = {"role": "agent", "content": full_response_text}
            if generated_sql:
                agent_message_content["sql_code"] = generated_sql
            if query_results_json is not None:
                agent_message_content["data_preview"] = query_results_json
            
            st.session_state[agent_choice].append(agent_message_content)
            # --- END NEW ---

# Placeholder for previous prompts (optional, can be removed)
# if execute_button and user_prompt: ...

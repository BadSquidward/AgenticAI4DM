# agents/data_mart_agent.py
import google.generativeai as genai
import streamlit as st
import re
import json
import pandas as pd

# นำเข้า Tools ที่ Agent สามารถเรียกใช้ได้
from tools.db_tools import execute_sql_query, get_table_schema
import config # สำหรับการเข้าถึง DATABASE_URL

class DataMartAgent:
    def __init__(self, api_key: str):
        """
        เริ่มต้น DataMartAgent ด้วย Gemini Pro Model และ Tools ที่กำหนด
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash-lite', # <--- ใช้ gemini-2.0-flash-lite ตามที่คุณแจ้ง
            tools=[execute_sql_query, get_table_schema] # DMA จะเน้นการ Query จาก DW
        )
        self.chat_session = self.model.start_chat(enable_automatic_function_calling=True)

    def process_prompt(self, user_prompt: str, st_response_container):
        """
        ประมวลผลคำสั่งจากผู้ใช้สำหรับ Data Mart Agent
        แสดงผลลัพธ์แบบ Streaming ผ่าน st_response_container

        Args:
            user_prompt (str): คำสั่งจากผู้ใช้
            st_response_container (st.empty): Streamlit container สำหรับแสดงผลลัพธ์แบบ Streaming

        Returns:
            tuple: (ข้อความตอบกลับทั้งหมด, SQL ที่สร้าง, ผลลัพธ์ Query ในรูปแบบ JSON string)
        """
        full_response_text = ""
        sql_generated = ""
        query_results_json = None

        context_prompt = (
            f"You are a Data Mart Agent. Your goal is to help create and query data marts for reporting. "
            f"You have access to a SQLite database at '{config.DATABASE_URL}' which serves as the Data Warehouse. "
            f"You can use the following tools: `execute_sql_query` to run SQL commands and `get_table_schema` to inspect table schemas. "
            f"Always use the provided database URL for all database operations."
        )
        self.chat_session.send_message(context_prompt)

        try:
            response_generator = self.chat_session.send_message(user_prompt, stream=True)

            current_display_text = ""
            for chunk in response_generator:
                if chunk.text:
                    current_display_text += chunk.text
                    st_response_container.markdown(current_display_text)

                if hasattr(chunk, 'function_calls') and chunk.function_calls:
                    for fc in chunk.function_calls:
                        tool_name = fc.name
                        tool_args = fc.args
                        tool_output_str = f"\n\n**Agent กำลังเรียกใช้ Tool:** `{tool_name}` พร้อม Arguments: `{tool_args}`\n"
                        st_response_container.markdown(current_display_text + tool_output_str)

                        actual_output = "Tool output not available yet."
                        # บังคับใช้ DATABASE_URL จาก config เสมอสำหรับ Tools ที่เกี่ยวข้องกับฐานข้อมูล
                        if tool_name in ["execute_sql_query", "get_table_schema"]:
                             tool_args['database_url'] = config.DATABASE_URL

                        # ทำการเรียกใช้ Tool จริงๆ
                        if tool_name == "execute_sql_query":
                            actual_output = execute_sql_query(**tool_args)
                            query_results_json = actual_output # เก็บผลลัพธ์ Query
                            full_response_text += tool_output_str + f"Tool Output:\n```json\n{actual_output}\n```\n"
                            st_response_container.markdown(current_display_text + tool_output_str + f"Tool Output:\n```json\n{actual_output}\n```\n")
                        elif tool_name == "get_table_schema":
                             actual_output = get_table_schema(**tool_args)
                             full_response_text += tool_output_str + f"Tool Output:\n```json\n{actual_output}\n```\n"
                             st_response_container.markdown(current_display_text + tool_output_str + f"Tool Output:\n```json\n{actual_output}\n```\n")
                        else:
                            actual_output = f"Tool `{tool_name}` ไม่รองรับโดย Data Mart Agent"
                            full_response_text += tool_output_str + actual_output
                            st_response_container.markdown(current_display_text + tool_output_str + actual_output)

                        # ส่งผลลัพธ์ของ Tool กลับไปยัง Gemini
                        self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
            
            # ดึงข้อความสรุปสุดท้ายจาก Gemini หลังจาก Tool Call
            final_response_chunk = self.chat_session.send_message("สรุปผลลัพธ์สุดท้ายของการดำเนินการทั้งหมดจากข้อมูล Tool output ที่ได้รับอย่างละเอียด", stream=True)
            for chunk in final_response_chunk:
                if chunk.text:
                    current_display_text += chunk.text
                    st_response_container.markdown(current_display_text)
            
            full_response_text = current_display_text

            # ตรวจสอบและดึง SQL Code ที่ Gemini อาจสร้างขึ้นในรูปแบบ Text
            sql_blocks = re.findall(r"```sql\n(.*?)```", full_response_text, re.DOTALL)
            if sql_blocks:
                sql_generated += "\n\n".join(sql_blocks)

            return full_response_text, sql_generated, query_results_json

        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการสื่อสารกับ Data Mart Agent: {e}")
            return f"เกิดข้อผิดพลาด: {str(e)}", "", ""


# agents/data_pipeline_agent.py
import google.generativeai as genai
import streamlit as st
import re
import json
import pandas as pd # Ensure pandas is imported for data processing/display

# นำเข้า Tools ที่ Agent สามารถเรียกใช้ได้
from tools.db_tools import execute_sql_query, get_db_engine, insert_data_into_table
from tools.file_tools import create_dataframe_from_csv_content
import config # สำหรับการเข้าถึง DATABASE_URL และ mock CSV content

class DataPipelineAgent:
    def __init__(self, api_key: str):
        """
        เริ่มต้น DataPipelineAgent ด้วย Gemini Pro Model และ Tools ที่กำหนด
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash-lite', # <--- ใช้ gemini-2.0-flash-lite ตามที่คุณแจ้ง
            tools=[execute_sql_query, create_dataframe_from_csv_content, insert_data_into_table]
        )
        # เริ่มต้น chat session และเปิดใช้งาน automatic function calling
        # Session นี้จะคงอยู่ตลอดอายุของ Agent instance
        self.chat_session = self.model.start_chat(enable_automatic_function_calling=True)

    def process_prompt(self, user_prompt: str, st_response_container):
        """
        ประมวลผลคำสั่งจากผู้ใช้โดยใช้ Gemini Pro และ Tools ที่กำหนด
        แสดงผลลัพธ์แบบ Streaming ผ่าน st_response_container

        Args:
            user_prompt (str): คำสั่งจากผู้ใช้
            st_response_container (st.empty): Streamlit container สำหรับแสดงผลลัพธ์แบบ Streaming

        Returns:
            tuple: (ข้อความตอบกลับทั้งหมดจาก Agent, รายการ Tool Output)
        """
        full_response_parts = [] # To accumulate all parts of the response for final display
        tool_outputs = []
        
        context_prompt = (
            f"You are a Data Pipeline Agent. Your goal is to help manage data pipelines. "
            f"You have access to a SQLite database at '{config.DATABASE_URL}'. "
            f"You can use the following tools: `execute_sql_query` to run SQL, "
            f"`create_dataframe_from_csv_content` to process CSV data, and "
            f"`insert_data_into_table` to load data into tables. "
            f"Always use the provided database URL for all database operations."
        )
        
        # Send context to LLM
        self.chat_session.send_message(context_prompt)

        try:
            # Send the user's prompt to Gemini Pro with stream=False
            response = self.chat_session.send_message(user_prompt, stream=False) # <--- Changed to stream=False

            # Process the single response object
            if response.text:
                full_response_parts.append(response.text)
                st_response_container.markdown("".join(full_response_parts)) # Update UI

            if hasattr(response, 'function_calls') and response.function_calls: # Check attribute on the full response
                for fc in response.function_calls:
                    tool_name = fc.name
                    tool_args = fc.args
                    tool_output_str = f"\n\n**Agent กำลังเรียกใช้ Tool:** `{tool_name}` พร้อม Arguments: `{tool_args}`\n"
                    full_response_parts.append(tool_output_str)
                    st_response_container.markdown("".join(full_response_parts)) # Update UI

                    actual_output = "Tool output not available yet."

                    # บังคับใช้ DATABASE_URL จาก config เสมอสำหรับ Tools ที่เกี่ยวข้องกับฐานข้อมูล
                    if tool_name in ["execute_sql_query", "insert_data_into_table", "get_table_schema", "create_table_ddl"]:
                        tool_args['database_url'] = config.DATABASE_URL

                    # ทำการเรียกใช้ Tool จริงๆ ตามชื่อฟังก์ชัน
                    if tool_name == "execute_sql_query":
                        actual_output = execute_sql_query(**tool_args)
                        full_response_parts.append(f"Tool Output:\n```json\n{actual_output}\n```\n")
                        st_response_container.markdown("".join(full_response_parts))

                    elif tool_name == "create_dataframe_from_csv_content":
                        if 'csv_content' not in tool_args:
                            if "customer" in user_prompt.lower():
                                tool_args['csv_content'] = config.MOCK_CUSTOMER_CSV
                            elif "sales" in user_prompt.lower():
                                tool_args['csv_content'] = config.MOCK_SALES_CSV
                            else:
                                actual_output = "ไม่พบเนื้อหา CSV ในคำสั่งหรือข้อมูลจำลอง"
                        
                        if 'csv_content' in tool_args:
                            actual_output = create_dataframe_from_csv_content(**tool_args)
                            full_response_parts.append(f"Tool Output:\n```json\n{actual_output}\n```\n")
                            st_response_container.markdown("".join(full_response_parts))

                            if "Error" not in actual_output and "DataFrame" in actual_output:
                                temp_msg = "\n" + f"**Agent กำลังพยายามโหลดข้อมูล CSV เข้า DB...**"
                                full_response_parts.append(temp_msg)
                                st_response_container.markdown("".join(full_response_parts))
                                try:
                                    df_json_str = actual_output
                                    table_name_match = re.search(r'(?:into|to)\s+(\w+)(?:\s+table)?', user_prompt.lower())
                                    if table_name_match:
                                        db_table_name = table_name_match.group(1)
                                        insert_result = insert_data_into_table(config.DATABASE_URL, db_table_name, df_json_str)
                                        actual_output += f"\n--- **โหลดข้อมูลเข้าตาราง `{db_table_name}` ผลลัพธ์:** {insert_result} ---"
                                        full_response_parts.append(f"\n--- **โหลดข้อมูลเข้าตาราง `{db_table_name}` ผลลัพธ์:** {insert_result} ---")
                                        st_response_container.markdown("".join(full_response_parts))
                                    else:
                                        actual_output += "\n--- **ไม่สามารถระบุชื่อตารางปลายทางสำหรับโหลดข้อมูลได้จากคำสั่ง: โปรดระบุชัดเจน เช่น 'load to stg_customers'** ---"
                                        full_response_parts.append("\n--- **ไม่สามารถระบุชื่อตารางปลายทางสำหรับโหลดข้อมูลได้จากคำสั่ง: โปรดระบุชัดเจน เช่น 'load to stg_customers'** ---")
                                        st_response_container.markdown("".join(full_response_parts))
                                except Exception as e:
                                    actual_output += f"\n--- **เกิดข้อผิดพลาดในการโหลดข้อมูลเข้า DB:** {str(e)} ---"
                                    full_response_parts.append(f"\n--- **เกิดข้อผิดพลาดในการโหลดข้อมูลเข้า DB:** {str(e)} ---")
                                    st_response_container.markdown("".join(full_response_parts))


                    elif tool_name == "insert_data_into_table":
                         actual_output = insert_data_into_table(**tool_args)
                         full_response_parts.append(f"Tool Output:\n```json\n{actual_output}\n```\n")
                         st_response_container.markdown("".join(full_response_parts))
                    else:
                        actual_output = f"Tool `{tool_name}` ไม่รองรับโดย Data Pipeline Agent"
                        full_response_parts.append(actual_output)
                        st_response_container.markdown("".join(full_response_parts))

                    # ส่งผลลัพธ์ของ Tool กลับไปยัง Gemini เพื่อให้ Agent ประมวลผลต่อ
                    self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                    tool_outputs.append({"name": tool_name, "args": tool_args, "output": actual_output})
            
            # ดึงข้อความสรุปสุดท้ายจาก Gemini หลังจาก Tool Call (also with stream=False)
            final_response = self.chat_session.send_message("สรุปผลลัพธ์สุดท้ายของการดำเนินการทั้งหมดจากข้อมูล Tool output ที่ได้รับอย่างละเอียด", stream=False) # <--- Changed to stream=False
            if final_response.text:
                full_response_parts.append(final_response.text)
                st_response_container.markdown("".join(full_response_parts))
            
            return "".join(full_response_parts), tool_outputs

        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการสื่อสารกับ Data Pipeline Agent: {e}")
            return f"เกิดข้อผิดพลาด: {str(e)}", []


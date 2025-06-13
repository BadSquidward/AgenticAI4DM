# agents/data_warehouse_agent.py
import google.generativeai as genai
from tools.db_tools import get_table_schema, create_table_ddl, execute_sql_query
import config
import streamlit as st
import re # For simple regex to extract DDL/DML

class DataWarehouseAgent:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash-lite',
            tools=[get_table_schema, create_table_ddl, execute_sql_query] # Add more DW specific tools
        )
        self.chat_session = self.model.start_chat(enable_automatic_function_calling=True)

    def process_prompt(self, user_prompt: str):
        st_placeholder = st.empty()
        full_response_text = ""
        sql_generated = ""

        try:
            response = self.chat_session.send_message(user_prompt)

            for chunk in response:
                if chunk.text:
                    full_response_text += chunk.text
                    st_placeholder.markdown(full_response_text)

                if hasattr(chunk, 'function_calls') and chunk.function_calls:
                    for fc in chunk.function_calls:
                        tool_name = fc.name
                        tool_args = fc.args
                        tool_output_str = f"Agent calling tool: `{tool_name}` with args: `{tool_args}`\n"
                        st_placeholder.markdown(full_response_text + "\n" + tool_output_str)

                        # Execute tool if it's a DB tool
                        if tool_name in ["get_table_schema", "create_table_ddl", "execute_sql_query"]:
                             if 'database_url' not in tool_args:
                                tool_args['database_url'] = config.DATABASE_URL
                             
                             if tool_name == "create_table_ddl" and 'schema_json' in tool_args:
                                actual_output = create_table_ddl(**tool_args)
                                sql_generated = actual_output # Capture DDL
                             else:
                                 # Generic execution for other DB tools
                                 tool_func = globals().get(tool_name)
                                 if tool_func:
                                     actual_output = tool_func(**tool_args)
                                 else:
                                     actual_output = f"Tool '{tool_name}' not found."

                             tool_output_str += f"Tool Output:\n```json\n{actual_output}\n```\n"
                             self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                             st_placeholder.markdown(full_response_text + "\n" + tool_output_str)
                        else:
                            st_placeholder.markdown(full_response_text + f"\nError: Tool `{tool_name}` not supported by DW Agent.")

            final_gemini_response = self.chat_session.send_message("What is the final summary after all operations?")
            full_response_text += "\n\n" + final_gemini_response.text

            # Extract any potential SQL code that Gemini might generate as text
            sql_blocks = re.findall(r"```sql\n(.*?)```", full_response_text, re.DOTALL)
            if sql_blocks:
                sql_generated = "\n\n".join(sql_blocks)


            st_placeholder.markdown(full_response_text)
            return full_response_text, sql_generated

        except Exception as e:
            st.error(f"Error communicating with Data Warehouse Agent: {e}")
            return "", ""
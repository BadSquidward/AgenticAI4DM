# agents/data_mart_agent.py
import streamlit as st
import google.generativeai as genai
from tools.db_tools import execute_sql_query, get_table_schema
import config
import re


class DataMartAgent:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            'gemini-pro',
            tools=[execute_sql_query, get_table_schema] # DMA will mainly query DW
        )
        self.chat_session = self.model.start_chat(enable_automatic_function_calling=True)

    def process_prompt(self, user_prompt: str):
        st_placeholder = st.empty()
        full_response_text = ""
        sql_generated = ""
        query_results_json = ""

        try:
            response = self.chat_session.send_message(user_prompt)

            for chunk in response:
                if chunk.text:
                    full_response_text += chunk.text
                    st_placeholder.markdown(full_response_text)

                if chunk.function_calls:
                    for fc in chunk.function_calls:
                        tool_name = fc.name
                        tool_args = fc.args
                        tool_output_str = f"Agent calling tool: `{tool_name}` with args: `{tool_args}`\n"
                        st_placeholder.markdown(full_response_text + "\n" + tool_output_str)

                        if tool_name == "execute_sql_query":
                            if 'database_url' not in tool_args:
                                tool_args['database_url'] = config.DATABASE_URL
                            actual_output = execute_sql_query(**tool_args)
                            tool_output_str += f"Tool Output:\n```json\n{actual_output}\n```\n"
                            query_results_json = actual_output # Capture query results
                            self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                            st_placeholder.markdown(full_response_text + "\n" + tool_output_str)
                        elif tool_name == "get_table_schema":
                             if 'database_url' not in tool_args:
                                tool_args['database_url'] = config.DATABASE_URL
                             actual_output = get_table_schema(**tool_args)
                             tool_output_str += f"Tool Output:\n```json\n{actual_output}\n```\n"
                             self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                             st_placeholder.markdown(full_response_text + "\n" + tool_output_str)
                        else:
                            st_placeholder.markdown(full_response_text + f"\nError: Tool `{tool_name}` not supported by DM Agent.")

            final_gemini_response = self.chat_session.send_message("What is the final summary after all operations?")
            full_response_text += "\n\n" + final_gemini_response.text

            # Extract any potential SQL code that Gemini might generate as text
            sql_blocks = re.findall(r"```sql\n(.*?)```", full_response_text, re.DOTALL)
            if sql_blocks:
                sql_generated = "\n\n".join(sql_blocks)

            st_placeholder.markdown(full_response_text)
            return full_response_text, sql_generated, query_results_json

        except Exception as e:
            st.error(f"Error communicating with Data Mart Agent: {e}")
            return "", "", ""
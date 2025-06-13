# agents/data_pipeline_agent.py
import google.generativeai as genai
import streamlit as st
from tools.db_tools import execute_sql_query, get_db_engine
from tools.file_tools import create_dataframe_from_csv_content
import config # Assuming config.py is in the parent directory
import re

class DataPipelineAgent:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash-lite',
            tools=[execute_sql_query, create_dataframe_from_csv_content]
        )
        self.chat_session = self.model.start_chat(enable_automatic_function_calling=True)

    def process_prompt(self, user_prompt: str):
        st_placeholder = st.empty() # Placeholder for streaming output
        full_response_text = ""
        tool_outputs = []

        try:
            # First, send the user's prompt
            response = self.chat_session.send_message(user_prompt)

            for chunk in response:
                if chunk.text:
                    full_response_text += chunk.text
                    st_placeholder.markdown(full_response_text) # Update text dynamically

                if hasattr(chunk, 'function_calls') and chunk.function_calls:
                    for fc in chunk.function_calls:
                        tool_name = fc.name
                        tool_args = fc.args
                        tool_output_str = f"Agent calling tool: `{tool_name}` with args: `{tool_args}`\n"
                        st_placeholder.markdown(full_response_text + "\n" + tool_output_str)

                        # Dynamically call the tool function
                        tool_function = globals().get(tool_name) # Get function from global scope (or pass specific tools)
                        if tool_function:
                            if tool_name == "execute_sql_query":
                                # Example: Append database_url if it's not in tool_args
                                if 'database_url' not in tool_args:
                                    tool_args['database_url'] = config.DATABASE_URL
                                actual_output = tool_function(**tool_args)
                                tool_output_str += f"Tool Output:\n```json\n{actual_output}\n```\n"
                                tool_outputs.append({"name": tool_name, "args": tool_args, "output": actual_output})
                                # Send tool output back to Gemini
                                self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                            elif tool_name == "create_dataframe_from_csv_content":
                                # This tool expects content directly, so we need to mock it or get from prompt
                                if 'csv_content' in user_prompt: # A very basic way to mock content
                                     csv_data_match = re.search(r'```csv\n(.*?)```', user_prompt, re.DOTALL)
                                     if csv_data_match:
                                         tool_args['csv_content'] = csv_data_match.group(1).strip()
                                     else: # Fallback to mock data from config if no CSV in prompt
                                         if "customer" in user_prompt.lower():
                                             tool_args['csv_content'] = config.MOCK_CUSTOMER_CSV
                                         elif "sales" in user_prompt.lower():
                                             tool_args['csv_content'] = config.MOCK_SALES_CSV
                                         else:
                                             tool_output_str += "Warning: No CSV content found in prompt or mock data."
                                             actual_output = "No CSV content for tool."

                                if 'csv_content' in tool_args:
                                    actual_output = tool_function(**tool_args)
                                    tool_output_str += f"Tool Output:\n```json\n{actual_output}\n```\n"
                                    tool_outputs.append({"name": tool_name, "args": tool_args, "output": actual_output})
                                    self.chat_session.send_message(genai.Part.from_function_response(name=tool_name, response=actual_output))
                                else:
                                    actual_output = "CSV content missing for tool."
                                    tool_output_str += actual_output

                            st_placeholder.markdown(full_response_text + "\n" + tool_output_str)
                        else:
                            st_placeholder.markdown(full_response_text + f"\nError: Tool `{tool_name}` not found.")

            # After all chunks are processed, get the final text response from Gemini
            final_gemini_response = self.chat_session.send_message("What is the final summary after all operations?")
            st_placeholder.markdown(full_response_text + "\n\n" + final_gemini_response.text)

            return full_response_text + "\n\n" + final_gemini_response.text, tool_outputs

        except Exception as e:
            st.error(f"Error communicating with Data Pipeline Agent: {e}")
            return "", []
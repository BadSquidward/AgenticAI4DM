# tools/file_tools.py
import pandas as pd
from io import StringIO
import json

def create_dataframe_from_csv_content(csv_content: str):
    """Creates a pandas DataFrame from a CSV content string."""
    try:
        df = pd.read_csv(StringIO(csv_content))
        return df.to_json(orient='records')
    except Exception as e:
        return f"Error creating DataFrame from CSV content: {str(e)}"

def save_dataframe_to_csv(df_json: str, file_path: str):
    """Saves a JSON-formatted DataFrame string to a CSV file."""
    try:
        df = pd.read_json(StringIO(df_json))
        df.to_csv(file_path, index=False)
        return f"DataFrame successfully saved to {file_path}"
    except Exception as e:
        return f"Error saving DataFrame to CSV: {str(e)}"
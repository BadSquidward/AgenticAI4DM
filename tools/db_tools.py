# tools/db_tools.py
from sqlalchemy import create_engine, text
import pandas as pd
import json

def get_db_engine(database_url: str):
    """Creates and returns a SQLAlchemy engine."""
    return create_engine(database_url)

def execute_sql_query(database_url: str, query: str):
    """Executes a given SQL query against the specified database URL and returns results."""
    try:
        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            result = connection.execute(text(query))
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df.to_json(orient='records') # Return as JSON string for LLM
            else:
                return "Query executed successfully with no rows returned."
    except Exception as e:
        return f"Error executing SQL query: {str(e)}"

def get_table_schema(database_url: str, table_name: str):
    """Returns the schema of a given table as a dictionary."""
    try:
        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            query = f"PRAGMA table_info({table_name});" # SQLite specific
            # For PostgreSQL: "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
            # For MySQL: "DESCRIBE {table_name};"
            result = connection.execute(text(query)).fetchall()
            schema = {row[1]: row[2] for row in result} # {column_name: data_type}
            return json.dumps(schema)
    except Exception as e:
        return f"Error getting table schema: {str(e)}"

def create_table_ddl(database_url: str, table_name: str, schema_json: str):
    """Creates a SQL DDL for table creation based on schema.
       Schema JSON example: {"id": "INTEGER PRIMARY KEY", "name": "TEXT"}
    """
    try:
        schema = json.loads(schema_json)
        columns_ddl = []
        for col_name, col_type in schema.items():
            columns_ddl.append(f"{col_name} {col_type}")
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_ddl)});"

        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            connection.execute(text(ddl))
            connection.commit()
        return f"Table '{table_name}' created or already exists."
    except Exception as e:
        return f"Error creating table DDL: {str(e)}"
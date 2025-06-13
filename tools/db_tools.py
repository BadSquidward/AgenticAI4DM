# tools/db_tools.py
from sqlalchemy import create_engine, text
import pandas as pd
import json

def get_db_engine(database_url: str):
    """
    สร้างและคืนค่า SQLAlchemy engine สำหรับเชื่อมต่อฐานข้อมูล
    """
    return create_engine(database_url)

def execute_sql_query(database_url: str, query: str):
    """
    รันคำสั่ง SQL ที่กำหนดกับฐานข้อมูลและส่งคืนผลลัพธ์ (ถ้ามี)
    """
    try:
        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            result = connection.execute(text(query))
            connection.commit() # Commit changes for DML operations

            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df.to_json(orient='records')
            else:
                return f"Query executed successfully with no rows returned. Rows affected: {result.rowcount}"
    except Exception as e:
        return f"Error executing SQL query: {str(e)}"

def get_table_schema(database_url: str, table_name: str):
    """
    ส่งคืน Schema ของตารางที่กำหนดในรูปแบบ Dictionary (column_name: data_type)
    """
    try:
        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            query = f"PRAGMA table_info({table_name});"
            result = connection.execute(text(query)).fetchall()
            schema = {row[1]: row[2] for row in result}
            return json.dumps(schema)
    except Exception as e:
        return f"Error getting table schema for '{table_name}': {str(e)}"

def create_table_ddl(database_url: str, table_name: str, schema_json: str):
    """
    สร้างและรันคำสั่ง SQL DDL (Data Definition Language) สำหรับการสร้างตาราง
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
        return f"Table '{table_name}' created or already exists. DDL: {ddl}"
    except Exception as e:
        return f"Error creating table DDL for '{table_name}': {str(e)}"

def insert_data_into_table(database_url: str, table_name: str, data_json: str): # <--- ฟังก์ชันนี้ต้องมีอยู่และสะกดถูกต้อง
    """
    แทรกข้อมูล JSON ลงในตารางที่กำหนด

    Args:
        database_url (str): URL สำหรับเชื่อมต่อฐานข้อมูล
        table_name (str): ชื่อตารางที่จะแทรกข้อมูล
        data_json (str): ข้อมูลที่จะแทรกในรูปแบบ JSON string ของ List of Dictionaries

    Returns:
        str: ข้อความยืนยันการแทรกข้อมูลหรือข้อผิดพลาด
    """
    try:
        # ใช้ StringIO เพื่อแปลง JSON string เป็นไฟล์เหมือนในหน่วยความจำ
        df = pd.read_json(json.dumps(json.loads(data_json)), orient='records') # ต้องแน่ใจว่า data_json เป็น JSON string ที่ถูกต้อง
        engine = get_db_engine(database_url)
        with engine.connect() as connection:
            # ใช้ to_sql เพื่อแทรกข้อมูล
            df.to_sql(table_name, con=connection, if_exists='append', index=False)
            connection.commit() # Commit transaction for data insertion
        return f"Successfully inserted {len(df)} rows into '{table_name}'."
    except Exception as e:
        return f"Error inserting data into '{table_name}': {str(e)}"

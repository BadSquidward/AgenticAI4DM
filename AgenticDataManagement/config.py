# config.py
import os

# Gemini API Key
# It's highly recommended to store this as an environment variable
# For prototype simplicity, you can hardcode it here temporarily,
# but for actual deployment, use os.getenv("GEMINI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY_HERE")

# Database URL for SQLite (for local prototype)
# You can change this to PostgreSQL, MySQL, etc., if needed
DATABASE_URL = "sqlite:///data/prototype.db"

# Ensure data directory exists
if not os.path.exists("data"):
    os.makedirs("data")

# Mock CSV Content for demonstration
MOCK_CUSTOMER_CSV = """
customer_id,name,email,registration_date,is_active
1,Alice Smith,alice@example.com,2023-01-15,True
2,Bob Johnson,bob@example.com,2023-02-20,False
3,Charlie Brown,charlie@example.com,2023-03-10,True
4,Diana Prince,diana@example.com,2023-04-05,True
"""

MOCK_SALES_CSV = """
order_id,customer_id,product_id,order_date,amount
101,1,P001,2024-05-01,150.00
102,3,P002,2024-05-01,200.00
103,1,P003,2024-05-02,50.00
104,2,P001,2024-05-02,100.00
105,4,P004,2024-05-03,300.00
"""
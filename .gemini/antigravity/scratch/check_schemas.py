import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

key_path = "/Users/as-mac-1288/sigma-genai-de/day6/bonus/student_key.p8"

with open(key_path, 'rb') as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user='student_genai',
    account='GEJKIOG-TKC55632',
    private_key=private_key_bytes,
    database='SIGMA_DE',
    schema='PUBLIC',
    warehouse='COMPUTE_WH',
    role='STUDENT_CORTEX'
)
cur = conn.cursor()

try:
    print("Checking grants to user student_genai...")
    cur.execute("SHOW GRANTS TO USER student_genai")
    for row in cur.fetchall():
        print(f"  Role granted: {row[1]}, Granted by: {row[4]}")
except Exception as e:
    print(f"Error checking user grants: {e}")

conn.close()

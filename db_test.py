import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def test_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT")
        )
        print("Database connection successful!")
        conn.close()
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")

test_db_connection()
import psycopg2

try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="mystrongpassword",
        host="localhost",  # Use 'localhost' if ports are mapped
        port="5432"        # Default PostgreSQL port
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
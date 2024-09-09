import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'postgres',   # Replace with your database name
    'user': 'postgres',       # Replace with your PostgreSQL username
    'password': 'password',   # Replace with your PostgreSQL password
    'host': 'localhost',           # Replace with your host (e.g., 'localhost')
    'port': 5433                   # Replace with the port if different (default is 5432)
}

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    
    # Create a cursor object
    cur = conn.cursor()

    # Query the table and fetch the results
    cur.execute("SELECT * FROM users;")
    rows = cur.fetchall()

    # Print the results
    for row in rows:
        print(row)

    # Close the cursor and connection
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")

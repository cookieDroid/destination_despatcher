import os
import psycopg2
from prettytable.colortable import ColorTable, Themes

def fetch_last_five_rows():
    try:
        # Connect to the PostgreSQL database
        postgres_url = os.environ['POSTGRES_CREDENTIAL']
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()

        # Query to fetch the last 5 rows from the table
        query = "SELECT * FROM elevator_events ORDER BY elevator_id DESC LIMIT 5;"
        cur.execute(query)

        # Fetch the results
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        # Print the results in table format
        table = ColorTable(theme=Themes.OCEAN)
        table.field_names = column_names
        for row in rows:
            table.add_row(row)

        print(table)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur:
            cur.close()

if __name__ == "__main__":
    fetch_last_five_rows()
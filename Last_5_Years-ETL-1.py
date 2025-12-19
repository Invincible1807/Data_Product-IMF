import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging
import time
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(filename="etl_pipeline.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# PostgreSQL connection parameters for both databases
DB_CONFIGS = {
    "transactional": {
        "dbname": "mf_transactional_db",
        "user": "postgres",
        "password": "adityA$853211",
        "host": "localhost",
        "port": "5432"
    },
    "analytical": {
        "dbname": "mf_analytical_db",
        "user": "postgres",
        "password": "adityA$853211",
        "host": "localhost",
        "port": "5432"
    }
}

# AMFI Historical NAV Data URL (alternative sources can be suggested)
NAV_DATA_URL = "https://www.amfiindia.com/spages/NAVAll.txt"


# Fetch NAV Data with retry mechanism
def fetch_nav_data(retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(NAV_DATA_URL, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                logging.error(f"Failed to fetch NAV data. Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}. Retrying ({attempt + 1}/{retries})...")
            time.sleep(delay)
    return None


# Process NAV data with filtering
def process_nav_data(data):
    lines = data.split("\n")[1:]  # Skip headers
    nav_records = []
    today = datetime.today().strftime("%Y-%m-%d")
    two_years_ago = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")

    for line in lines:
        cols = line.strip().split(";")
        if len(cols) < 5:
            continue

        try:
            scheme_code = cols[0].strip()
            scheme_name = cols[1].strip() or "Unknown Scheme"
            net_asset_value = float(cols[4].strip()) if cols[4].strip() else None
            nav_date = cols[5].strip() if len(cols) > 5 else today

            # Ensure valid date
            if not scheme_code or not nav_date:
                continue  # Skip records with essential data missing

            nav_records.append((scheme_code, scheme_name, net_asset_value, nav_date, two_years_ago))

        except ValueError as e:
            logging.warning(f"Error parsing line: {line}. Error: {e}")

    return nav_records


# Insert into PostgreSQL with different retention policies
def insert_nav_data(nav_records):
    conn_trans = conn_ana = None
    try:
        conn_trans = psycopg2.connect(**DB_CONFIGS["transactional"])
        conn_ana = psycopg2.connect(**DB_CONFIGS["analytical"])
        cursor_trans = conn_trans.cursor()
        cursor_ana = conn_ana.cursor()

        two_years_ago = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")

        history_inserted_ana = 0
        history_inserted_trans = 0

        for record in nav_records:
            scheme_code, scheme_name, net_asset_value, nav_date, two_years_ago = record

            # Insert into analytical database (all data)
            cursor_ana.execute("""
                INSERT INTO mf_price_history (scheme_code, scheme_name, net_asset_value, nav_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (scheme_code, nav_date) DO NOTHING;
            """, (scheme_code, scheme_name, net_asset_value, nav_date))
            history_inserted_ana += cursor_ana.rowcount

            # Insert into transactional database (only last 2 years)
            if nav_date >= two_years_ago:
                cursor_trans.execute("""
                    INSERT INTO mf_prices (scheme_code, scheme_name, net_asset_value, nav_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (scheme_code) DO UPDATE
                    SET net_asset_value = EXCLUDED.net_asset_value, nav_date = EXCLUDED.nav_date;
                """, (scheme_code, scheme_name, net_asset_value, nav_date))
                history_inserted_trans += cursor_trans.rowcount

        conn_trans.commit()
        conn_ana.commit()

        logging.info(f"New records inserted into mf_analytical_db: {history_inserted_ana}")
        logging.info(f"New records inserted into mf_transactional_db: {history_inserted_trans}")

        print(f"New records inserted into mf_analytical_db: {history_inserted_ana}")
        print(f"New records inserted into mf_transactional_db: {history_inserted_trans}")

        # Export the data back to Excel
        export_to_excel(conn_trans, conn_ana)

    except Exception as e:
        logging.error(f"Database error: {e}")
        print("Database error:", e)

    finally:
        if conn_trans:
            conn_trans.close()
        if conn_ana:
            conn_ana.close()


# Export data to Excel
def export_to_excel(conn_trans, conn_ana):
    try:
        # Fetch data from both tables
        query_history = "SELECT * FROM mf_price_history;"
        query_current = "SELECT * FROM mf_prices;"

        df_history = pd.read_sql(query_history, conn_ana)
        df_current = pd.read_sql(query_current, conn_trans)

        # Define the file path for Excel export
        file_path = r"C:\Users\Aditya Patel\OneDrive\EXTRAS\Intellect\mutual_fund_data.xlsx"

        # Save the data to an Excel file
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df_history.to_excel(writer, sheet_name="Historical NAV", index=False)
            df_current.to_excel(writer, sheet_name="Current NAV", index=False)

        logging.info(f"Excel file '{file_path}' created successfully.")
        print(f"Excel file '{file_path}' created successfully.")

    except Exception as e:
        logging.error(f"Error exporting data to Excel: {e}")
        print("Error exporting data to Excel:", e)


# Validate Database Schema
def validate_database_schema():
    conn_trans = conn_ana = None
    try:
        conn_trans = psycopg2.connect(**DB_CONFIGS["transactional"])
        conn_ana = psycopg2.connect(**DB_CONFIGS["analytical"])
        cursor_trans = conn_trans.cursor()
        cursor_ana = conn_ana.cursor()

        # Check required tables in transactional DB
        cursor_trans.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name IN ('mf_prices');
        """)
        trans_tables = [row[0] for row in cursor_trans.fetchall()]

        if "mf_prices" not in trans_tables:
            raise Exception("Table mf_prices not found in mf_transactional_db.")

        # Check required tables in analytical DB
        cursor_ana.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name IN ('mf_price_history');
        """)
        ana_tables = [row[0] for row in cursor_ana.fetchall()]

        if "mf_price_history" not in ana_tables:
            raise Exception("Table mf_price_history not found in mf_analytical_db.")

        logging.info("Database schema validation passed.")
        print("Database schema validation passed.")

    except Exception as e:
        logging.error(f"Database schema validation failed: {e}")
        print("Database schema validation failed:", e)

    finally:
        if conn_trans:
            conn_trans.close()
        if conn_ana:
            conn_ana.close()


# Run the ETL Pipeline
# if __name__ == "__main__":
#     logging.info("ETL pipeline started.")
#
#     # Validate database before running ETL
#     validate_database_schema()
#
#     data = fetch_nav_data()
#     if data:
#         nav_records = process_nav_data(data)
#         insert_nav_data(nav_records)
#
#     logging.info("ETL pipeline completed.")


# ✅ Runs all functions in sequence:
# 1️⃣ Database Validation
# 2️⃣ Data Fetching
# 3️⃣ Data Processing
# 4️⃣ Data Insertion
# 5️⃣ Data Export

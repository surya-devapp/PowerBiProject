import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import warnings
import streamlit as st
import time

# Suppress warnings related to chained assignment/regex usage in Pandas
warnings.filterwarnings('ignore')

# --- MySQL Connection Parameters ---
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Surya@123'
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_DB = 'real_estate_db'
FILE_PATH = "D:\ReealEstate_Project\PowerBiProject\Luxury_Housing_Bangalore.csv" # Input data file

# --- Database Connection Functions (Optimized with Caching) ---

@st.cache_resource
def get_db_connection_cached():
    """Establishes and returns a cached connection to the target MySQL database."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        # Added timeout for faster failure detection
        connection_timeout=5 
    )

def create_database(status_placeholder):
    """Creates the 'real_estate_db' database if it doesn't already exist.
       Uses a non-cached connection for the initial database creation."""
    status_placeholder.info(f"Attempting to connect to MySQL and create database '{MYSQL_DB}'...")
    
    # Use a direct, non-cached connection temporarily, as we can't specify the DB name yet
    connection = None
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            # Added timeout for faster failure detection
            connection_timeout=5
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
        status_placeholder.success(f"✅ Database '{MYSQL_DB}' created or already exists.")
        return True
    except Error as e:
        status_placeholder.error(f"❌ Connection Error during database creation. Error: {e}")
        return False
    finally:
        if connection:
            connection.close()
            # Clear the resource cache to force the next connection call to recognize the new DB
            get_db_connection_cached.clear() 

def create_table(status_placeholder):
    """Creates the 'luxury_housing_bangalore' table using the cached connection."""
    status_placeholder.info(f"Creating table 'luxury_housing_bangalore'...")
    connection = None
    cursor = None
    try:
        connection = get_db_connection_cached() # Reuses cached connection
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS luxury_housing_bangalore (
            Property_ID VARCHAR(50) PRIMARY KEY,
            Micro_Market VARCHAR(100),
            Project_Name VARCHAR(150),
            Developer_Name VARCHAR(150),
            Unit_Size_Sqft FLOAT,
            Configuration VARCHAR(50),
            Ticket_Price_Cr FLOAT,
            Transaction_Type VARCHAR(50),
            Buyer_Type VARCHAR(50),
            Purchase_Quarter DATETIME,
            Connectivity_Score FLOAT,
            Amenity_Score FLOAT,
            Possession_Status VARCHAR(50),
            Sales_Channel VARCHAR(100),
            NRI_Buyer VARCHAR(10),
            Locality_Infra_Score FLOAT,
            Avg_Traffic_Time_Min INT,
            Buyer_Comments LONGTEXT,
            Price_per_Sqft FLOAT,
            Quarter_Number INT,
            BHK_Count FLOAT,
            Booking_Status VARCHAR(10)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        status_placeholder.success("✅ Table 'luxury_housing_bangalore' created or already exists.")
        return True
    except Error as e:
        status_placeholder.error(f"❌ Error creating table: {e}")
        return False
    finally:
        if cursor: cursor.close()
        # NOTE: Connection is NOT closed here because it is managed by @st.cache_resource

# --- Data Preparation Function ---

# Use st.cache_data to cache the result of the expensive data processing
@st.cache_data
def clean_data(file_path):
    """
    Loads, cleans, standardizes, imputes, and engineers features on the raw housing data.
    Returns the cleaned Pandas DataFrame (cached).
    """
    st.info(f"Loading and processing raw data from '{file_path}' (this runs only once)...")
    try:
        data = pd.read_csv(file_path)
    except FileNotFoundError:
        st.error(f"❌ Error: Input file '{file_path}' not found.")
        return pd.DataFrame()

    # --- Cleaning Logic (Unchanged) ---
    data = data.drop_duplicates()
    data['Ticket_Price_Cr'] = (data['Ticket_Price_Cr'].astype(str).str.replace(r'[₹Cr\s]', '', regex=True).str.strip().replace('nan', np.nan))
    data['Ticket_Price_Cr'] = pd.to_numeric(data['Ticket_Price_Cr'], errors='coerce').round(3).fillna(0)
    data['Unit_Size_Sqft'] = data['Unit_Size_Sqft'].mask(data['Unit_Size_Sqft'] < 0, 0).fillna(0)
    data['Buyer_Comments'] = data['Buyer_Comments'].fillna("")
    average_amenity_score = data["Amenity_Score"].mean()
    data["Amenity_Score"] = data["Amenity_Score"].fillna(average_amenity_score)
    data['Micro_Market'] = data['Micro_Market'].str.title().str.strip()
    data['Configuration'] = data['Configuration'].str.upper().str.strip()
    if 'Buyer_Type' in data.columns:
        data['Buyer_Type'] = data['Buyer_Type'].str.title().str.strip()
    data['Booking_Status'] = np.where(data['Ticket_Price_Cr'] > 0, 'Booked', 'Not Booked')
    data['Price_per_Sqft'] = np.where(data['Unit_Size_Sqft'] > 0, (data['Ticket_Price_Cr'] * 10000000) / data['Unit_Size_Sqft'], 0).round(3)
    data['Purchase_Quarter'] = pd.to_datetime(data['Purchase_Quarter'], errors='coerce')
    data['Quarter_Number'] = data['Purchase_Quarter'].dt.quarter.fillna(0).astype(int)
    if 'Configuration' in data.columns:
        data['BHK_Count'] = (data['Configuration'].str.extract('(\d+)', expand=False).astype(float))
    for col in data.select_dtypes(include=[np.number]).columns:
        if np.isinf(data[col]).sum() > 0:
            data.replace([np.inf, -np.inf], 0, inplace=True)

    return data

# --- Data Insertion Function (using cached connection) ---

def insert_data(data, status_placeholder):
    """Inserts the cleaned DataFrame directly into the main MySQL table."""
    if data.empty:
        status_placeholder.error("❌ Cannot insert: DataFrame is empty.")
        return

    status_placeholder.info(f"Starting batched insertion of {len(data)} rows (Luxury Housing)...")

    connection = None
    cursor = None
    try:
        connection = get_db_connection_cached() # Reuses cached connection
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO luxury_housing_bangalore (
            Property_ID, Micro_Market, Project_Name, Developer_Name, Unit_Size_Sqft,
            Configuration, Ticket_Price_Cr, Transaction_Type, Buyer_Type, Purchase_Quarter,
            Connectivity_Score, Amenity_Score, Possession_Status, Sales_Channel, NRI_Buyer,
            Locality_Infra_Score, Avg_Traffic_Time_Min, Buyer_Comments, Price_per_Sqft,
            Quarter_Number, BHK_Count, Booking_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch_size = 1000
        values_list = []

        for index, row in data.iterrows():
            values = (
                row.get('Property_ID'), row.get('Micro_Market'), row.get('Project_Name'), row.get('Developer_Name'), row.get('Unit_Size_Sqft'),
                row.get('Configuration'), row.get('Ticket_Price_Cr'), row.get('Transaction_Type'), row.get('Buyer_Type'),
                row['Purchase_Quarter'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['Purchase_Quarter']) else None,
                row.get('Connectivity_Score'), row.get('Amenity_Score'), row.get('Possession_Status'), row.get('Sales_Channel'),
                row.get('NRI_Buyer'), row.get('Locality_Infra_Score'), row.get('Avg_Traffic_Time_Min'),
                row['Buyer_Comments'] if pd.notna(row['Buyer_Comments']) else None,
                row.get('Price_per_Sqft'), row.get('Quarter_Number'), row.get('BHK_Count'), row.get('Booking_Status')
            )
            values_list.append(values)

            if len(values_list) == batch_size:
                cursor.executemany(insert_query, values_list)
                connection.commit()
                values_list = []

        if values_list:
            cursor.executemany(insert_query, values_list)
            connection.commit()
            
        status_placeholder.success(f"✅ Successfully inserted all {len(data)} rows into 'luxury_housing_bangalore'.")

    except Error as e:
        status_placeholder.error(f"❌ Error inserting housing data: {e}")
    finally:
        if cursor: cursor.close()
        # Connection is NOT closed here because it is managed by @st.cache_resource

def insert_locality_data(status_placeholder):
    """
    Creates the 'microMarket_locations' table and inserts/updates geographical coordinates.
    """
    status_placeholder.info("Inserting Locality Geographical Data...")
    
    data_dict = {
        "Locality": ["Bannerghatta Road", "Bellary Road", "Domlur", "Electronic City", "Hebbal", "Hennur Road", "Indiranagar", "Jayanagar", "JP Nagar", "Kanakapura Road", "Koramangala", "MG Road", "Rajajinagar", "Sarjapur Road", "Whitefield", "Yelahanka"],
        "Latitude": [12.8715, 13.0652, 12.9687, 12.8468, 13.0374, 13.0559, 12.9733, 12.9255, 12.9056, 12.8942, 12.9345, 12.9768, 12.9961, 12.9152, 12.9698, 13.1007],
        "Longitude": [77.5925, 77.5941, 77.6409, 77.6713, 77.5912, 77.6617, 77.6408, 77.5843, 77.5796, 77.5683, 77.6266, 77.6047, 77.5524, 77.7126, 77.7499, 77.5963]
    }
    df_localities = pd.DataFrame(data_dict)
    TABLE_NAME = 'microMarket_locations'
    
    connection = None
    cursor = None
    try:
        connection = get_db_connection_cached() # Reuses cached connection
        cursor = connection.cursor()
        
        # Create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            Locality VARCHAR(255) PRIMARY KEY,
            Latitude DECIMAL(10, 8),
            Longitude DECIMAL(11, 8)
        )
        """
        cursor.execute(create_table_query)

        # Define the INSERT query structure (using ON DUPLICATE KEY UPDATE for upsert)
        insert_query = f"""
        INSERT INTO {TABLE_NAME} (Locality, Latitude, Longitude)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE Latitude = VALUES(Latitude), Longitude = VALUES(Longitude)
        """
        data_to_insert = [tuple(row) for row in df_localities.values]
        cursor.executemany(insert_query, data_to_insert)
        
        connection.commit()
        status_placeholder.success(f"✅ Successfully inserted/updated {len(df_localities)} locality records into '{TABLE_NAME}'.")

    except Error as e:
        status_placeholder.error(f"❌ Error inserting locality data: {e}")
    finally:
        if cursor: cursor.close()
        # Connection is NOT closed here because it is managed by @st.cache_resource

# --- SQL Validation Queries ---

def run_validation_queries():
    """Executes the final validation queries and displays results in Streamlit."""
    st.markdown("---")
    st.header("4. SQL Validation Queries")
    st.info("Executing validation queries against the loaded MySQL data...")

    connection = None
    cursor = None
    try:
        connection = get_db_connection_cached() # Reuses cached connection
        cursor = connection.cursor()

        queries = {
            "Total Row Count (luxury_housing_bangalore)": "SELECT COUNT(*) FROM luxury_housing_bangalore",
            "Booking Status Distribution": "SELECT Booking_Status, COUNT(*) as Total FROM luxury_housing_bangalore GROUP BY Booking_Status",
            "Average Ticket Price per Developer": "SELECT Developer_Name, AVG(Ticket_Price_Cr) AS Avg_Price_Cr FROM luxury_housing_bangalore GROUP BY Developer_Name ORDER BY Avg_Price_Cr DESC LIMIT 10"
        }

        for title, query in queries.items():
            st.subheader(title)
            st.code(query, language='sql')
            cursor.execute(query)
            
            # Fetch data and column names
            columns = [i[0] for i in cursor.description]
            data = cursor.fetchall()
            df_result = pd.DataFrame(data, columns=columns)
            
            st.dataframe(df_result, use_container_width=True)

    except Error as e:
        st.error(f"❌ Error during SQL validation: {e}")
    finally:
        if cursor: cursor.close()
        # Connection is NOT closed here because it is managed by @st.cache_resource

# --- Streamlit Main Function ---

def main():
    st.set_page_config(page_title="ETL Pipeline Runner", layout="wide")
    st.title("🏡 Luxury Housing ETL and Validation Pipeline (Optimized)")
    st.markdown("---")
    st.warning(f"Using MySQL Database: `{MYSQL_DB}` on `{MYSQL_HOST}:{MYSQL_PORT}`. Ensure MySQL is running.")

    # Status containers for dynamic updates
    # We define status placeholders inside the button click for cleaner component management
    
    if st.button("🚀 Run Full ETL Pipeline & Validation"):
        
        # Define placeholders before the execution starts
        status_db_setup = st.empty()
        status_table_house = st.empty()
        status_clean = st.empty()
        status_insert_house = st.empty()
        status_insert_geo = st.empty()
        
        start_time = time.time()
        
        # 1. Database Setup
        st.header("1. Database Setup")
        if not create_database(status_db_setup):
            return

        if not create_table(status_table_house):
            return

        # 2. Data Preparation
        st.header("2. Data Preparation & Feature Engineering")
        
        # Call clean_data once. If successful, cached_df holds the result.
        with st.spinner("Processing data..."):
            cleaned_df = clean_data(FILE_PATH)
        
        # Replace the status_clean placeholder content now that processing is done
        if cleaned_df.empty:
            status_clean.error("❌ Data cleaning failed (check file path or content).")
            return
        status_clean.success(f"✅ Data Preparation Complete! Cleaned shape: {cleaned_df.shape}")


        # Display sample of cleaned data
        st.subheader("Sample of Cleaned Data (with New Features)")
        st.dataframe(cleaned_df.head(), use_container_width=True)

        # 3. Data Insertion
        st.placeholder("Data Loading into MySQL")
        
        # Insert Housing Data
        insert_data(cleaned_df, status_insert_house)
        
        # Insert Locality Data
        insert_locality_data(status_insert_geo)
        
        # 4. Run SQL Validation Queries
        run_validation_queries()

        end_time = time.time()
        st.balloons()
        st.success(f"🎉 Pipeline execution complete in {end_time - start_time:.2f} seconds!")


if __name__ == "__main__":
    main()
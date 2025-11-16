# Real Estate Data Pipeline & Power BI Analytics

A comprehensive end-to-end data pipeline for processing luxury housing data in Bangalore, with MySQL database integration and Power BI visualization capabilities.

## 📋 Project Overview

This project automates the extraction, transformation, and loading (ETL) of real estate data from CSV files into a MySQL database, followed by visualization through Power BI dashboards. The pipeline focuses on luxury housing in Bangalore, performing data cleaning, feature engineering, and geographical data management.

## ✨ Features

- **Automated Data Cleaning**: Removes duplicates, standardizes formats, and handles missing values
- **Feature Engineering**: Creates derived metrics like price per sqft, booking status, and BHK count
- **MySQL Integration**: Direct database connectivity for seamless data storage and retrieval
- **Geographical Data Management**: Manages micro-market locations with latitude/longitude coordinates
- **Power BI Ready**: Cleaned and processed data structured for Power BI visualization
- **Error Handling**: Robust error management and data validation throughout the pipeline

## 🏗️ Project Structure

```
PowerBiProject/
├── data_clean.py                    # Main ETL script
├── PowerBIProject.pbix              # Power BI dashboard file
├── Luxury_Housing_Bangalore.csv     # Input data (CSV file)
├── README.md                        # This file
└── pyvenv.cfg                       # Python virtual environment config
```

## 🗄️ Database Schema

### Tables Created

#### 1. `luxury_housing_bangalore`
Main table storing luxury housing transaction data with the following columns:

| Column Name | Data Type | Description |
|---|---|---|
| Property_ID | VARCHAR(50) | Unique identifier (Primary Key) |
| Micro_Market | VARCHAR(100) | Neighborhood/locality |
| Project_Name | VARCHAR(150) | Real estate project name |
| Developer_Name | VARCHAR(150) | Developer/builder name |
| Unit_Size_Sqft | FLOAT | Property size in square feet |
| Configuration | VARCHAR(50) | BHK configuration (e.g., 2BHK, 3BHK) |
| Ticket_Price_Cr | FLOAT | Price in crores |
| Transaction_Type | VARCHAR(50) | Type of transaction |
| Buyer_Type | VARCHAR(50) | Individual, NRI, Corporate, etc. |
| Purchase_Quarter | DATETIME | Date of purchase |
| Connectivity_Score | FLOAT | Connectivity rating |
| Amenity_Score | FLOAT | Amenities rating |
| Possession_Status | VARCHAR(50) | Ready/Under Construction status |
| Sales_Channel | VARCHAR(100) | Sales channel used |
| NRI_Buyer | VARCHAR(10) | Whether buyer is NRI |
| Locality_Infra_Score | FLOAT | Infrastructure score |
| Avg_Traffic_Time_Min | INT | Average traffic time in minutes |
| Buyer_Comments | LONGTEXT | Buyer feedback/comments |
| Price_per_Sqft | FLOAT | Engineered: Price per square foot |
| Quarter_Number | INT | Engineered: Quarter number |
| BHK_Count | FLOAT | Engineered: Number of bedrooms |
| Booking_Status | VARCHAR(10) | Engineered: Booked/Not Booked |

#### 2. `microMarket_locations`
Geographical data for micro-markets:

| Column Name | Data Type | Description |
|---|---|---|
| Micro_Market | VARCHAR(255) | Micro-market name (Primary Key) |
| Latitude | DECIMAL(10, 8) | Geographic latitude |
| Longitude | DECIMAL(11, 8) | Geographic longitude |

## 🚀 Installation & Setup

### Prerequisites

- Python 3.7+
- MySQL Server 5.7+
- pandas, numpy, mysql-connector-python

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd PowerBiProject
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux
```

### Step 3: Install Dependencies

```bash
pip install pandas numpy mysql-connector-python
```

### Step 4: Configure Database Connection

Edit `data_clean.py` and update the MySQL connection parameters:


### Step 5: Prepare Data

Place your CSV file (`Luxury_Housing_Bangalore.csv`) in the project root directory.

## 📊 Data Cleaning Pipeline

The pipeline performs the following transformations:

### 1. Initial Cleanup
- Remove duplicate records
- Remove negative values from Unit_Size_Sqft
- Handle missing values in Buyer_Comments

### 2. Data Standardization
- **Ticket_Price_Cr**: Remove currency symbols, convert to numeric
- **Text Fields**: Title case for Micro_Market, uppercase for Configuration
- **Dates**: Convert Purchase_Quarter to datetime format

### 3. Missing Value Imputation
- **Amenity_Score**: Filled with mean value of the column

### 4. Feature Engineering
- **Price_per_Sqft**: Calculated as (Price_Cr * 10,000,000) / Unit_Size_Sqft
- **Quarter_Number**: Extracted from Purchase_Quarter date
- **BHK_Count**: Extracted from Configuration field (e.g., "2BHK" → 2)
- **Booking_Status**: Derived from Ticket_Price_Cr (Booked if > 0)

### 5. Validation
- Handle infinite values and replace with 0

## 🔄 Running the Pipeline

Execute the complete ETL pipeline:

```bash
python data_clean.py
```

### Output

The script will:
1. ✅ Create the MySQL database (if not exists)
2. ✅ Create required tables
3. ✅ Clean and transform the CSV data
4. ✅ Insert 1000s of records in batches
5. ✅ Load geographical/locality data
6. ✅ Provide success/error messages for each step

Example output:
```
Starting combined E2E Data Pipeline...

--- Starting Data Preparation ---
Initial shape: (5000, 20)
Cleaned shape: (4950, 23)
--- Data Preparation Complete ---

✅ Database 'real_estate_db' created or already exists
✅ Table 'luxury_housing_bangalore' created or already exists
--- Starting Data Insertion of 4950 rows (Luxury Housing) ---
✅ Successfully inserted all 4950 rows into MySQL
--- Starting Locality Data Insertion ---
✅ Successfully inserted/updated 16 locality records into 'microMarket_locations'

✅ E2E Data Pipeline execution complete!
```

## 📈 Power BI Integration

Once data is loaded into MySQL, connect Power BI to visualize:

1. **Open PowerBIProject.pbix** in Power BI Desktop
2. **Configure Data Source**:
   - Add MySQL connection
   - Select `real_estate_db` database
   - Query the tables

### Sample Visualizations
- Price distribution by micro-market
- Average price per sqft trends
- Connectivity and amenity scores comparison
- Booking status analysis
- Transaction volume by quarter

## 🛠️ Key Functions

### `clean_data(file_path)`
Loads and cleans CSV data with all transformations applied.

**Parameters**: `file_path` (str) - Path to CSV file  
**Returns**: Cleaned pandas DataFrame

### `create_database()`
Creates the `real_estate_db` database if it doesn't exist.

### `create_table()`
Creates the `luxury_housing_bangalore` table with full schema.

### `insert_data(data)`
Inserts cleaned DataFrame records into MySQL in batches of 1000.

**Parameters**: `data` (DataFrame) - Cleaned data to insert

### `insert_locality_data()`
Creates and populates the `microMarket_locations` table with geographical coordinates for 16 Bangalore micro-markets.

## ⚠️ Important Notes

- **Sensitive Information**: Store database credentials securely (use environment variables in production)
- **Batch Processing**: Data is inserted in batches of 1000 rows for optimal performance
- **Data Validation**: The script includes validation to handle edge cases and infinite values
- **CSV Format**: Ensure input CSV matches expected column names and data types

## 🔧 Troubleshooting

### MySQL Connection Error
- Verify MySQL server is running
- Check host, port, username, and password
- Ensure database user has appropriate permissions

### FileNotFoundError
- Verify `Luxury_Housing_Bangalore.csv` exists in project root
- Check file name spelling and case sensitivity

### Data Type Errors
- Ensure CSV columns match expected data types
- Check for unexpected characters or formatting in numeric columns

## 👥 Contributors

- **Owner**: surya-devapp
- **Repository**: PowerBiProject

**Last Updated**: November 2025  
**Python Version**: 3.7+  
**Status**: Active Development

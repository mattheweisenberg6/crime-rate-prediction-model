import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime
import os

# Database configuration - update with your credentials
DB_CONFIG = {
    'host': 'localhost',
    'database': 'phoenix_crime_db',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),  # Update this
    'port': '5432'
}

def create_connection():
    """Create database connection"""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def explore_csv(filename, num_rows=10):
    """First, let's see what we're working with"""
    print("=== CSV EXPLORATION ===")
    
    # Load a sample to understand structure
    try:
        df = pd.read_csv(filename, nrows=num_rows)
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print("\nFirst few rows:")
        print(df.head())
        print("\nData types:")
        print(df.dtypes)
        print("\nSample values:")
        for col in df.columns:
            print(f"{col}: {df[col].iloc[0] if len(df) > 0 else 'No data'}")
        return df.columns.tolist()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None

def clean_data(df):
    """Clean the CSV data for database import"""
    print("\n=== DATA CLEANING ===")
    
    # Create our database column mapping
    column_mapping = {
        'INC NUMBER': 'incident_id',
        'UCR CRIME CATEGORY': 'crime_type',
        'OCCURRED ON': 'occurred_date', 
        '100 BLOCK ADDR': 'address',
        'ZIP': 'zip_code',
        'PREMISE TYPE': 'premise_type',
        'GRID': 'grid_id'
    }
    
    print(f"Original columns: {df.columns.tolist()}")
    
    # Keep only columns we want and rename them
    available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df[list(available_columns.keys())].copy()
    df = df.rename(columns=available_columns)
    
    print(f"Mapped columns: {df.columns.tolist()}")
    print(f"Starting with {len(df)} records")
    
    # Clean incident_id
    if 'incident_id' in df.columns:
        df['incident_id'] = df['incident_id'].astype(str).str.strip()
        # Remove any rows with missing incident_id
        initial_count = len(df)
        df = df[df['incident_id'].notna() & (df['incident_id'] != '') & (df['incident_id'] != 'nan')]
        print(f"Removed {initial_count - len(df)} records with missing incident_id")
    
    # Clean dates - they're already formatted correctly
    if 'occurred_date' in df.columns:
        df['occurred_date'] = pd.to_datetime(df['occurred_date'], errors='coerce')
        
        # Remove any invalid dates (just in case)
        date_nulls = df['occurred_date'].isnull().sum()
        if date_nulls > 0:
            df = df.dropna(subset=['occurred_date'])
            print(f"Removed {date_nulls} records with invalid dates")
        else:
            print("All dates parsed successfully")
    
    # Clean text columns
    text_columns = ['crime_type', 'address', 'premise_type', 'grid_id']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
    
    # Clean zip codes
    if 'zip_code' in df.columns:
        df['zip_code'] = df['zip_code'].astype(str).str.extract('(\d{5})')[0]
    
    # Remove duplicates based on incident_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['incident_id'], keep='first')
    print(f"Removed {initial_count - len(df)} duplicate incident_ids")
    
    print(f"Final dataset: {len(df)} records")
    
    if len(df) > 0:
        print(f"Date range: {df['occurred_date'].min()} to {df['occurred_date'].max()}")
        print(f"Crime types: {df['crime_type'].nunique()} unique types")
    
    return df

def load_to_database(df, batch_size=1000):
    """Load cleaned data to PostgreSQL"""
    print(f"\n=== DATABASE LOADING ===")
    
    engine = create_connection()
    if not engine:
        return False
    
    try:
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM crimes"))
            existing_count = result.fetchone()[0]
            print(f"Current records in database: {existing_count}")
        
        # Load data in batches
        total_records = len(df)
        total_batches = (total_records + batch_size - 1) // batch_size
        
        print(f"Loading {total_records} records in {total_batches} batches...")
        
        successful_records = 0
        
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i+batch_size].copy()
            batch_num = (i // batch_size) + 1
            
            try:
                # Use pandas to_sql with conflict handling
                batch.to_sql(
                    'crimes', 
                    engine, 
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                successful_records += len(batch)
                print(f"? Batch {batch_num}/{total_batches}: {len(batch)} records")
                
            except Exception as e:
                print(f"? Batch {batch_num} failed: {e}")
                # Try individual records in this batch
                for _, record in batch.iterrows():
                    try:
                        record.to_frame().T.to_sql('crimes', engine, if_exists='append', index=False)
                        successful_records += 1
                    except Exception as record_error:
                        print(f"  Failed record {record['incident_id']}: {record_error}")
        
        print(f"\n? Successfully loaded {successful_records}/{total_records} records")
        
        # Verify final count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM crimes"))
            final_count = result.fetchone()[0]
            print(f"Total records now in database: {final_count}")
        
        return True
        
    except Exception as e:
        print(f"Database loading error: {e}")
        return False
    
    finally:
        engine.dispose()

def verify_data():
    """Quick verification of loaded data"""
    print(f"\n=== VERIFICATION ===")
    
    engine = create_connection()
    if not engine:
        return
    
    try:
        with engine.connect() as conn:
            # Basic stats
            queries = {
                'Total records': "SELECT COUNT(*) FROM crimes",
                'Date range': "SELECT MIN(occurred_date), MAX(occurred_date) FROM crimes",
                'Crime types': "SELECT COUNT(DISTINCT crime_type) FROM crimes"
            }
            
            for description, query in queries.items():
                result = conn.execute(text(query))
                data = result.fetchone()
                print(f"{description}: {data}")
            
            # Top crime types
            print("\nTop 5 crime types:")
            result = conn.execute(text("""
                SELECT crime_type, COUNT(*) as count 
                FROM crimes 
                GROUP BY crime_type 
                ORDER BY count DESC 
                LIMIT 5
            """))
            
            for row in result:
                print(f"  {row[0]}: {row[1]}")
    
    except Exception as e:
        print(f"Verification error: {e}")
    
    finally:
        engine.dispose()

def main():
    """Main loading process"""
    # Configuration
    CSV_FILENAME = "crimes.csv"  # Update this to your CSV file path
    
    print("?? Starting CSV to PostgreSQL import process")
    
    # Step 1: Explore the CSV
    print(f"\nStep 1: Exploring {CSV_FILENAME}")
    columns = explore_csv(CSV_FILENAME, num_rows=5)
    if not columns:
        print("? Failed to read CSV file")
        return
    
    # Ask user if they want to continue
    response = input("\nDoes the data look correct? Continue with full import? (y/n): ")
    if response.lower() != 'y':
        print("Import cancelled")
        return
    
    # Step 2: Load and clean full dataset
    print(f"\nStep 2: Loading full dataset")
    try:
        df = pd.read_csv(CSV_FILENAME)
        print(f"Loaded {len(df)} total records")
    except Exception as e:
        print(f"Error loading full CSV: {e}")
        return
    
    # Step 3: Clean the data
    df_clean = clean_data(df)
    if len(df_clean) == 0:
        print("? No valid records after cleaning")
        return
    
    # Step 4: Load to database
    success = load_to_database(df_clean)
    
    # Step 5: Verify
    if success:
        verify_data()
        print("\n?? Import completed successfully!")
    else:
        print("\n? Import failed")

if __name__ == "__main__":
    main()
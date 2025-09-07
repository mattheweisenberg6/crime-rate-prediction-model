import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import requests
import schedule
import time
import logging
from datetime import datetime, timedelta
import os
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('phoenix_api_updater.log'),
        logging.StreamHandler()
    ]
)

class PhoenixCrimeAPIUpdater:
    """
    Phoenix Crime Data Updater using CKAN API
    Much more efficient than downloading entire CSV files!
    
    Benefits:
    - Incremental updates via date filtering
    - Faster (only fetch new records)
    - More reliable (API vs file downloads)
    - Real-time metadata access
    """
    
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'database': 'phoenix_crime_db',
            'user': 'postgres',
            'password': os.getenv('DB_PASSWORD'),
            'port': '5432'
        }
        
        # Phoenix CKAN API endpoints
        self.api_base = "https://www.phoenixopendata.com/api/3/action"
        self.resource_id = "0ce3411a-2fc6-4302-a33f-167f68608a20"  # Crime data resource ID
        
        # Phoenix column mapping
        self.column_mapping = {
            'INC NUMBER': 'incident_id',
            'UCR CRIME CATEGORY': 'crime_type',
            'OCCURRED ON': 'occurred_date',
            'OCCURRED TO': 'occurred_to_date',
            '100 BLOCK ADDR': 'address',
            'ZIP': 'zip_code',
            'PREMISE TYPE': 'premise_type',
            'GRID': 'grid_id'
        }
        
        self.status_file = 'api_update_status.json'

    def create_connection(self):
        """Create database connection"""
        try:
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            engine = create_engine(connection_string)
            return engine
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return None

    def get_dataset_metadata(self):
        """Get metadata about the Phoenix crime dataset"""
        try:
            url = f"{self.api_base}/resource_show"
            params = {'id': self.resource_id}
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data['success']:
                resource_info = data['result']
                return {
                    'name': resource_info.get('name', 'Crime Data'),
                    'description': resource_info.get('description', ''),
                    'last_modified': resource_info.get('last_modified'),
                    'created': resource_info.get('created'),
                    'format': resource_info.get('format'),
                    'size': resource_info.get('size')
                }
            else:
                logging.error(f"API returned error: {data}")
                return None
                
        except Exception as e:
            logging.error(f"Failed to get metadata: {e}")
            return None

    def get_latest_db_date(self):
        """Get the most recent occurred_date in database"""
        engine = self.create_connection()
        if not engine:
            return None
            
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT MAX(occurred_date) FROM crimes"))
                latest_date = result.fetchone()[0]
                return latest_date
        except Exception as e:
            logging.error(f"Error getting latest date: {e}")
            return None
        finally:
            engine.dispose()

    def fetch_new_records(self, since_date=None, limit=10000, offset=0):
        """
        Fetch new crime records from Phoenix API
        
        Args:
            since_date: Only get records after this date
            limit: Number of records per API call (max 32000)
            offset: Starting offset for pagination
        """
        try:
            url = f"{self.api_base}/datastore_search"
            
            params = {
                'resource_id': self.resource_id,
                'limit': min(limit, 32000),  # Phoenix API limit
                'offset': offset
            }
            
            # Add date filter if specified
            if since_date:
                # Format date for Phoenix API (CKAN uses PostgreSQL-style queries)
                date_str = since_date.strftime('%Y-%m-%d')
                params['filters'] = json.dumps({
                    'OCCURRED ON': f'>{date_str}'
                })
                logging.info(f"Fetching records newer than {date_str}")
            
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            
            if data['success']:
                result = data['result']
                records = result['records']
                total = result['total']
                
                logging.info(f"API call successful: {len(records)} records fetched (total available: {total})")
                return records, total
            else:
                logging.error(f"API returned error: {data}")
                return [], 0
                
        except Exception as e:
            logging.error(f"API fetch failed: {e}")
            return [], 0

    def fetch_all_new_records(self, since_date=None):
        """
        Fetch all new records using pagination
        """
        all_records = []
        offset = 0
        limit = 10000  # Reasonable batch size
        
        logging.info("Starting to fetch new records from Phoenix API...")
        
        while True:
            records, total = self.fetch_new_records(since_date, limit, offset)
            
            if not records:
                break
                
            all_records.extend(records)
            offset += limit
            
            logging.info(f"Progress: {len(all_records):,} / {total:,} records fetched")
            
            # If we got fewer records than requested, we're done
            if len(records) < limit:
                break
        
        logging.info(f"✅ Completed: {len(all_records):,} total records fetched")
        return all_records

    def clean_api_data(self, records):
        """Clean data received from Phoenix API"""
        if not records:
            return pd.DataFrame()
            
        logging.info(f"Cleaning {len(records):,} records from API...")
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Map columns to our database schema
        available_columns = {k: v for k, v in self.column_mapping.items() if k in df.columns}
        df = df[list(available_columns.keys())].copy()
        df = df.rename(columns=available_columns)
        
        # Clean incident_id
        df['incident_id'] = df['incident_id'].astype(str).str.strip()
        df = df[df['incident_id'].notna() & (df['incident_id'] != '') & (df['incident_id'] != 'nan')]
        
        # Clean dates
        if 'occurred_date' in df.columns:
            df['occurred_date'] = pd.to_datetime(df['occurred_date'], errors='coerce')
            df = df.dropna(subset=['occurred_date'])
        
        if 'occurred_to_date' in df.columns:
            df['occurred_to_date'] = pd.to_datetime(df['occurred_to_date'], errors='coerce')
        
        # Clean text columns
        text_columns = ['crime_type', 'address', 'premise_type', 'grid_id']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'NaN', '', 'None'], None)
        
        # Clean ZIP codes
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str.extract('(\d{5})')[0]
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['incident_id'], keep='first')
        
        logging.info(f"✅ Cleaned data: {len(df):,} valid records")
        return df

    def update_database(self, df):
        """Update database with new records"""
        if len(df) == 0:
            logging.info("No records to insert")
            return True
            
        logging.info(f"Updating database with {len(df):,} records...")
        
        engine = self.create_connection()
        if not engine:
            return False
            
        try:
            # Check for existing records to avoid duplicates
            with engine.connect() as conn:
                incident_ids = df['incident_id'].tolist()
                
                # Build query to check existing IDs
                placeholders = ', '.join([f"'{id}'" for id in incident_ids])
                query = f"SELECT incident_id FROM crimes WHERE incident_id IN ({placeholders})"
                
                result = conn.execute(text(query))
                existing_ids = {row[0] for row in result}
            
            # Filter out existing records
            if existing_ids:
                initial_count = len(df)
                df = df[~df['incident_id'].isin(existing_ids)]
                filtered_count = initial_count - len(df)
                logging.info(f"Filtered out {filtered_count:,} existing records")
            
            if len(df) == 0:
                logging.info("All records already exist in database")
                return True
            
            # Insert new records
            df.to_sql('crimes', engine, if_exists='append', index=False, method='multi')
            
            logging.info(f"✅ Successfully inserted {len(df):,} new records")
            
            # Verify count
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM crimes"))
                total_count = result.fetchone()[0]
                logging.info(f"Total records in database: {total_count:,}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Database update failed: {e}")
            return False
        finally:
            engine.dispose()

    def run_api_update(self):
        """Run the complete API-based update process"""
        start_time = datetime.now()
        logging.info("=" * 60)
        logging.info(f"🚀 Starting Phoenix API Update - {start_time}")
        
        status = {
            'start_time': start_time,
            'method': 'CKAN API',
            'success': False,
            'records_fetched': 0,
            'records_inserted': 0,
            'error': None
        }
        
        try:
            # Step 1: Get dataset metadata
            metadata = self.get_dataset_metadata()
            if metadata:
                logging.info(f"📊 Dataset: {metadata['name']}")
                logging.info(f"   Last modified: {metadata['last_modified']}")
                status['dataset_last_modified'] = metadata['last_modified']
            
            # Step 2: Get latest date from database for incremental update
            latest_db_date = self.get_latest_db_date()
            
            # Use a small buffer (1 day) to catch any updates to existing records
            if latest_db_date:
                since_date = latest_db_date - timedelta(days=1)
                logging.info(f"Incremental update since: {since_date}")
            else:
                since_date = None
                logging.info("Full initial load (no existing data)")
            
            # Step 3: Fetch new records from API
            records = self.fetch_all_new_records(since_date)
            status['records_fetched'] = len(records)
            
            if not records:
                status['success'] = True
                status['message'] = "No new records available"
                logging.info("✅ Update completed - No new data")
                return True
            
            # Step 4: Clean and process data
            df = self.clean_api_data(records)
            status['records_cleaned'] = len(df)
            
            # Step 5: Update database
            success = self.update_database(df)
            status['records_inserted'] = len(df) if success else 0
            
            if success:
                status['success'] = True
                status['message'] = f"Successfully processed {len(df):,} new records"
                logging.info("✅ Phoenix API update completed successfully!")
            else:
                raise Exception("Database update failed")
                
        except Exception as e:
            status['error'] = str(e)
            logging.error(f"❌ API update failed: {e}")
        
        finally:
            status['end_time'] = datetime.now()
            status['duration'] = str(status['end_time'] - start_time)
            
            # Save status
            try:
                with open(self.status_file, 'w') as f:
                    json.dump(status, f, indent=2, default=str)
            except Exception as e:
                logging.error(f"Failed to save status: {e}")
        
        return status['success']

    def get_record_count(self):
        """Get total record count from Phoenix API (for monitoring)"""
        try:
            records, total = self.fetch_new_records(limit=1)  # Just get count
            return total
        except Exception as e:
            logging.error(f"Failed to get record count: {e}")
            return None

    def schedule_updates(self):
        """Schedule regular updates"""
        # Schedule every 4 hours (more frequent since API is efficient)
        schedule.every(4).hours.do(self.run_api_update)
        
        # Also schedule daily at 11:30 AM
        schedule.every().day.at("11:30").do(self.run_api_update)
        
        logging.info("📅 API updater scheduled:")
        logging.info("   - Every 4 hours for frequent updates")
        logging.info("   - Daily at 11:30 AM (after Phoenix updates)")
        logging.info("   Press Ctrl+C to stop")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logging.info("⏹️  Scheduler stopped")

# Test function to explore the API
def test_api_connection():
    """Test connection to Phoenix API and show sample data"""
    updater = PhoenixCrimeAPIUpdater()
    
    print("🔍 Testing Phoenix API connection...")
    
    # Get metadata
    metadata = updater.get_dataset_metadata()
    if metadata:
        print("✅ Metadata retrieved:")
        for key, value in metadata.items():
            print(f"   {key}: {value}")
    
    # Get sample records
    print("\n📊 Fetching sample records...")
    records, total = updater.fetch_new_records(limit=5)
    
    if records:
        print(f"✅ API working! Total records available: {total:,}")
        print("\nSample record structure:")
        sample = records[0]
        for key, value in sample.items():
            print(f"   {key}: {value}")
    else:
        print("❌ No records retrieved")

# Convenience functions
def run_update_once():
    """Run API update once"""
    updater = PhoenixCrimeAPIUpdater()
    return updater.run_api_update()

def start_scheduler():
    """Start the API updater scheduler"""
    updater = PhoenixCrimeAPIUpdater()
    updater.schedule_updates()

if __name__ == "__main__":
    import sys
    
    if not os.getenv('DB_PASSWORD'):
        print("❌ DB_PASSWORD environment variable not set!")
        sys.exit(1)
    
    print("🚔 Phoenix Crime API Updater")
    print("=" * 40)
    print("1. Test API connection")
    print("2. Run update once")
    print("3. Start scheduler")
    print("4. Exit")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == "1":
        test_api_connection()
    elif choice == "2":
        success = run_update_once()
        print(f"Result: {'✅ Success' if success else '❌ Failed'}")
    elif choice == "3":
        start_scheduler()
    elif choice == "4":
        print("👋 Goodbye!")
    else:
        print("❌ Invalid choice")
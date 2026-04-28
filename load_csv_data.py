"""
Load CSV data into PostgreSQL database
Optimized with batch inserts and better error handling
"""
import csv
import os
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("csv-loader")

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")

def parse_timestamp(timestamp_str):
    """Parse timestamp string, handling various formats"""
    try:
        timestamp_str = timestamp_str.strip()
        # Try ISO format first
        if 'T' in timestamp_str:
            # Handle Z timezone indicator (UTC)
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            # Parse ISO format
            dt = datetime.fromisoformat(timestamp_str)
            # Make timezone-aware if naive (assume UTC)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        else:
            # Try other common formats (assume UTC for naive timestamps)
            try:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # Try with microseconds
                dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
            # Make timezone-aware (assume UTC)
            return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
        raise

def load_csv_to_db(csv_file):
    """Load CSV file into PostgreSQL database with batch processing"""
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        return False
    
    conn = None
    try:
        # Connect to database
        logger.info(f"Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Check if table exists and create if needed
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'cleanroom_logs'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.info("Creating cleanroom_logs table...")
            cur.execute("""
                CREATE TABLE cleanroom_logs (
                    id SERIAL PRIMARY KEY,
                    particles INT NOT NULL,
                    temperature REAL NOT NULL,
                    humidity REAL NOT NULL,
                    pressure REAL NOT NULL,
                    door_status TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    attack_status TEXT NOT NULL DEFAULT 'NORMAL',
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_time ON cleanroom_logs (timestamp DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_particles ON cleanroom_logs (particles);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_attack ON cleanroom_logs (attack_status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_temp ON cleanroom_logs (temperature);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_humidity ON cleanroom_logs (humidity);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cleanroom_pressure ON cleanroom_logs (pressure);")
            conn.commit()
            logger.info("✅ Table created successfully")
        else:
            logger.info("✅ Table already exists")
        
        # Count existing records
        cur.execute("SELECT COUNT(*) FROM cleanroom_logs;")
        existing_count = cur.fetchone()[0]
        logger.info(f"Existing records in database: {existing_count}")
        
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_file}")
        rows_to_insert = []
        errors = []
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_rows = 0
            valid_rows = 0
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
                total_rows += 1
                try:
                    # Validate and parse data
                    particles = int(row['particles'])
                    temperature = float(row['temperature'])
                    humidity = float(row['humidity'])
                    pressure = float(row['pressure'])
                    door_status = row['door_status'].strip()
                    user_id = row['user_id'].strip()
                    attack_status = row.get('attack_status', 'NORMAL').strip() or 'NORMAL'
                    timestamp = parse_timestamp(row['timestamp'].strip())
                    
                    # Validate door_status
                    if door_status not in ['OPEN', 'CLOSED']:
                        logger.warning(f"Row {row_num}: Invalid door_status '{door_status}', defaulting to 'CLOSED'")
                        door_status = 'CLOSED'
                    
                    # Prepare data tuple
                    rows_to_insert.append((
                        particles,
                        temperature,
                        humidity,
                        pressure,
                        door_status,
                        user_id,
                        attack_status,
                        timestamp
                    ))
                    valid_rows += 1
                    
                    # Show progress for large files
                    if total_rows % 1000 == 0:
                        logger.info(f"  Processed {total_rows} rows...")
                    
                except KeyError as e:
                    errors.append(f"Row {row_num}: Missing column {e}")
                except ValueError as e:
                    errors.append(f"Row {row_num}: Invalid value - {e}")
                except Exception as e:
                    errors.append(f"Row {row_num}: {str(e)}")
        
        logger.info(f"✅ Parsed {valid_rows} valid rows from {total_rows} total rows")
        if errors:
            logger.warning(f"⚠️  Encountered {len(errors)} errors (first 5 shown):")
            for error in errors[:5]:
                logger.warning(f"  {error}")
        
        if not rows_to_insert:
            logger.warning("No valid rows to insert!")
            return False
        
        # Batch insert using execute_batch for better performance
        logger.info(f"Inserting {len(rows_to_insert)} records into database...")
        insert_query = """
            INSERT INTO cleanroom_logs 
            (particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Use execute_batch for efficient bulk inserts (page_size = 1000)
        try:
            # Insert all records in batches
            execute_batch(cur, insert_query, rows_to_insert, page_size=1000)
            conn.commit()
            
            # Count inserted records
            cur.execute("SELECT COUNT(*) FROM cleanroom_logs;")
            new_count = cur.fetchone()[0]
            inserted = new_count - existing_count
            
            logger.info(f"✅ Successfully inserted {inserted} new records")
            logger.info(f"✅ Total records in database: {new_count}")
            
            if inserted < len(rows_to_insert):
                skipped = len(rows_to_insert) - inserted
                logger.info(f"ℹ️  {skipped} records may have been duplicates or failed to insert")
            
            return True
            
        except psycopg2.IntegrityError as e:
            logger.error(f"Database integrity error: {e}")
            conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Error during batch insert: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
            if conn:
                conn.close()
    
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        logger.error("Please check your DATABASE_URL in .env file")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        if conn:
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("🏭 Cleanroom IoT - CSV Data Loader")
    print("=" * 60)
    
    # Check DATABASE_URL
    if not DATABASE_URL or DATABASE_URL == "":
        logger.error("❌ DATABASE_URL not set in .env file!")
        logger.error("   Please create .env file from env.example and configure it")
        exit(1)
    
    # Load CSV files
    csv_files = ["cleanroom_synthetic.csv", "cleanroom_team.csv"]
    success_count = 0
    
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            logger.info(f"\n📁 Processing: {csv_file}")
            if load_csv_to_db(csv_file):
                success_count += 1
        else:
            logger.warning(f"⚠️  CSV file not found: {csv_file}")
    
    print("\n" + "=" * 60)
    if success_count > 0:
        logger.info(f"✅ CSV data loading complete! ({success_count}/{len(csv_files)} files loaded)")
    else:
        logger.error("❌ No files were successfully loaded!")
    print("=" * 60)

"""
CSV Data Streamer
Continuously streams CSV data to simulate real-time sensor readings
This feeds the database with data from CSV files at regular intervals
"""
import csv
import time
import logging
import random
from datetime import datetime, timezone
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CSVStreamer")

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", 5.0))  # seconds between records
CSV_FILES = ["cleanroom_synthetic.csv", "cleanroom_team.csv"]
SHUFFLE_DATA = os.getenv("SHUFFLE_DATA", "false").lower() == "true"  # Randomize order
LOOP_DATA = os.getenv("LOOP_DATA", "true").lower() == "true"  # Loop when reaching end

# PostgreSQL connection pool
pg_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)

def get_pg_conn():
    conn = pg_pool.getconn()
    if not conn:
        raise Exception("Database connection pool exhausted")
    return conn

def release_pg_conn(conn):
    pg_pool.putconn(conn)

def parse_timestamp(timestamp_str):
    """Parse timestamp string"""
    try:
        timestamp_str = timestamp_str.strip()
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
        return datetime.now(timezone.utc)

def load_csv_data(csv_file):
    """Load all records from CSV file into memory"""
    records = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Parse and validate data
                    record = {
                        "particles": int(row['particles']),
                        "temperature": float(row['temperature']),
                        "humidity": float(row['humidity']),
                        "pressure": float(row['pressure']),
                        "door_status": row['door_status'].strip(),
                        "user_id": row['user_id'].strip(),
                        "attack_status": row.get('attack_status', 'NORMAL').strip() or 'NORMAL',
                        "timestamp": parse_timestamp(row['timestamp'].strip())
                    }
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Skipping invalid row: {e}")
                    continue
        logger.info(f"✅ Loaded {len(records)} records from {csv_file}")
        return records
    except Exception as e:
        logger.error(f"Error loading {csv_file}: {e}")
        return []

def insert_record(record):
    """Insert a single record into database"""
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Update timestamp to current time for real-time effect
        current_timestamp = datetime.now(timezone.utc)
        
        cur.execute("""
            INSERT INTO cleanroom_logs 
            (particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record["particles"],
            record["temperature"],
            record["humidity"],
            record["pressure"],
            record["door_status"],
            record["user_id"],
            record["attack_status"],
            current_timestamp  # Use current time instead of CSV timestamp
        ))
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        return True
    except Exception as e:
        logger.error(f"Error inserting record: {e}")
        if conn:
            conn.rollback()
            release_pg_conn(conn)
        return False

def stream_csv_data():
    """Stream CSV data continuously to database"""
    # Load all CSV files
    all_records = []
    for csv_file in CSV_FILES:
        if os.path.exists(csv_file):
            records = load_csv_data(csv_file)
            all_records.extend(records)
        else:
            logger.warning(f"CSV file not found: {csv_file}")
    
    if not all_records:
        logger.error("No records to stream! Please ensure CSV files exist.")
        return
    
    logger.info(f"📊 Total records loaded: {len(all_records)}")
    
    # Shuffle if requested
    if SHUFFLE_DATA:
        random.shuffle(all_records)
        logger.info("🔀 Data shuffled")
    
    logger.info(f"🚀 Starting data stream (interval: {STREAM_INTERVAL}s)")
    logger.info(f"🔄 Loop mode: {'ON' if LOOP_DATA else 'OFF'}")
    logger.info("Press Ctrl+C to stop")
    
    index = 0
    total_inserted = 0
    
    try:
        while True:
            if index >= len(all_records):
                if LOOP_DATA:
                    logger.info("🔄 Reached end of data, looping back to start...")
                    index = 0
                    if SHUFFLE_DATA:
                        random.shuffle(all_records)
                else:
                    logger.info("✅ Finished streaming all records")
                    break
            
            record = all_records[index]
            
            # Insert record
            if insert_record(record):
                total_inserted += 1
                logger.info(
                    f"📤 [{total_inserted}] Streamed: "
                    f"Temp={record['temperature']:.1f}°C, "
                    f"Hum={record['humidity']:.1f}%, "
                    f"Press={record['pressure']:.1f}Pa, "
                    f"Particles={record['particles']}"
                )
            else:
                logger.warning(f"Failed to insert record {index}")
            
            index += 1
            time.sleep(STREAM_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info(f"\n🛑 Stream stopped. Total records inserted: {total_inserted}")

if __name__ == "__main__":
    # Check database connection
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cleanroom_logs;")
        existing_count = cur.fetchone()[0]
        cur.close()
        release_pg_conn(conn)
        logger.info(f"✅ Database connected. Existing records: {existing_count}")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("Please check your DATABASE_URL in .env file")
        exit(1)
    
    stream_csv_data()


"""
Setup script to initialize the Cleanroom IoT Monitoring System
"""
import os
import sys
import subprocess
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_dependencies():
    """Check if required Python packages are installed"""
    print("🔍 Checking dependencies...")
    required = ['fastapi', 'uvicorn', 'psycopg2', 'sqlalchemy', 'asyncpg']
    missing = []
    
    for package in required:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print("   Install with: pip install -r requirements.txt")
        return False
    
    print("✅ All dependencies installed")
    return True

def check_database():
    """Check database connection"""
    print("\n🔍 Checking database connection...")
    try:
        DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"✅ Database connected: {version.split(',')[0]}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("   Please check your DATABASE_URL in .env file")
        return False

def check_table():
    """Check if table exists"""
    print("\n🔍 Checking database table...")
    try:
        DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'cleanroom_logs'
            );
        """)
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        if exists:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cleanroom_logs;")
            count = cur.fetchone()[0]
            cur.close()
            print(f"✅ Table exists with {count} records")
            return True
        else:
            print("⚠️  Table does not exist (will be created when you load data)")
            return False
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return False

def check_env():
    """Check if .env file exists"""
    print("\n🔍 Checking environment configuration...")
    if os.path.exists(".env"):
        print("✅ .env file exists")
        return True
    else:
        print("⚠️  .env file not found")
        if os.path.exists("env.example"):
            print("   Copy env.example to .env and configure it")
        return False

def main():
    print("=" * 50)
    print("🏭 Cleanroom IoT Monitoring System - Setup Check")
    print("=" * 50)
    
    all_ok = True
    
    # Check environment
    if not check_env():
        all_ok = False
    
    # Check dependencies
    if not check_dependencies():
        all_ok = False
        sys.exit(1)
    
    # Check database
    if not check_database():
        all_ok = False
        sys.exit(1)
    
    # Check table
    check_table()
    
    print("\n" + "=" * 50)
    if all_ok:
        print("✅ Setup check complete!")
        print("\nNext steps:")
        print("1. Load CSV data: python load_csv_data.py")
        print("2. Start backend: python main.py")
        print("3. Open dashboard: http://localhost:8000/")
    else:
        print("⚠️  Please fix the issues above before proceeding")
    print("=" * 50)

if __name__ == "__main__":
    main()


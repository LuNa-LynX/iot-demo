"""
Scheduled Cloud Backup System for EMS Data
Automated backup of PostgreSQL data to cloud storage (AWS S3, Azure Blob, etc.)
"""
import os
import json
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import time
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CloudBackup")

# Configuration
BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "true").lower() == "true"
BACKUP_SCHEDULE = os.getenv("BACKUP_SCHEDULE", "daily")  # daily, hourly, weekly
BACKUP_TIME = os.getenv("BACKUP_TIME", "02:00")  # Time for daily backup (HH:MM)
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", 30))

# Cloud Storage Configuration
# Note: Cloud storage is OPTIONAL. Default is LOCAL backups (FREE).
# Cloud options require paid accounts:
# - AWS S3: Free tier = 5GB for 12 months, then paid (~$0.023 per GB/month)
# - Azure Blob: Free tier = 5GB for 12 months, then paid
# Recommendation: Use LOCAL backups (free, unlimited)
CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER", "local")  # local (FREE), aws (PAID), azure (PAID)
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
DB_NAME = DATABASE_URL.split("/")[-1].split("?")[0] if DATABASE_URL else "cleanroom_db"
DB_HOST = DATABASE_URL.split("@")[1].split(":")[0] if "@" in DATABASE_URL else "localhost"
DB_USER = DATABASE_URL.split("://")[1].split(":")[0] if "://" in DATABASE_URL else "postgres"
DB_PASSWORD = DATABASE_URL.split(":")[2].split("@")[0] if ":" in DATABASE_URL else ""

# Encryption Key
ENCRYPTION_KEY = os.getenv("BACKUP_ENCRYPTION_KEY", Fernet.generate_key().decode())
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "./backups"))
BACKUP_DIR.mkdir(exist_ok=True)

def encrypt_file(file_path, key):
    """Encrypt backup file"""
    try:
        fernet = Fernet(key)
        with open(file_path, 'rb') as f:
            data = f.read()
        encrypted_data = fernet.encrypt(data)
        encrypted_path = file_path.with_suffix(file_path.suffix + '.encrypted')
        with open(encrypted_path, 'wb') as f:
            f.write(encrypted_data)
        return encrypted_path
    except Exception as e:
        logger.error(f"Encryption error: {e}")
        return None

def calculate_file_hash(file_path):
    """Calculate SHA256 hash for integrity verification"""
    import hashlib
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def backup_database():
    """Perform database backup"""
    if not BACKUP_ENABLED:
        logger.info("Backup is disabled")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"cleanroom_db_backup_{timestamp}.sql"
    backup_path = BACKUP_DIR / backup_filename
    
    try:
        logger.info(f"🔄 Starting database backup...")
        
        # Create pg_dump command
        pg_dump_cmd = [
            "pg_dump",
            "-h", DB_HOST,
            "-U", DB_USER,
            "-d", DB_NAME,
            "-F", "c",  # Custom format
            "-f", str(backup_path)
        ]
        
        # Set password via environment
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_PASSWORD
        
        # Execute backup
        result = subprocess.run(pg_dump_cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Backup failed: {result.stderr}")
            return
        
        # Calculate file hash for integrity
        file_hash = calculate_file_hash(backup_path)
        
        # Encrypt backup
        encrypted_path = encrypt_file(backup_path, ENCRYPTION_KEY.encode())
        
        # Create manifest file
        manifest = {
            "backup_date": datetime.now().isoformat(),
            "backup_file": backup_filename,
            "encrypted_file": encrypted_path.name if encrypted_path else None,
            "file_size": backup_path.stat().st_size,
            "sha256_hash": file_hash,
            "database": DB_NAME
        }
        
        manifest_path = BACKUP_DIR / f"manifest_{timestamp}.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"✅ Backup completed: {backup_filename}")
        logger.info(f"   Size: {backup_path.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info(f"   Hash: {file_hash[:16]}...")
        
        # Upload to cloud storage (OPTIONAL - requires paid cloud account)
        if CLOUD_PROVIDER == "aws" and AWS_S3_BUCKET and AWS_ACCESS_KEY:
            logger.info("☁️  Attempting cloud upload (requires AWS credentials)...")
            upload_to_s3(encrypted_path, manifest_path)
        elif CLOUD_PROVIDER == "azure":
            logger.info("☁️  Azure upload not yet implemented (use local or AWS)")
        else:
            logger.info("💾 Backup stored locally (FREE, no cloud account needed)")
            logger.info(f"   Location: {BACKUP_DIR}")
            logger.info("   💡 Tip: Copy backups to external drive or network share for offsite backup")
        
        # Cleanup old backups
        cleanup_old_backups()
        
    except Exception as e:
        logger.error(f"Backup error: {e}", exc_info=True)

def upload_to_s3(file_path, manifest_path):
    """Upload backup to AWS S3"""
    try:
        import boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        
        s3_key = f"backups/{file_path.name}"
        s3_client.upload_file(str(file_path), AWS_S3_BUCKET, s3_key)
        
        manifest_key = f"backups/{manifest_path.name}"
        s3_client.upload_file(str(manifest_path), AWS_S3_BUCKET, manifest_key)
        
        logger.info(f"☁️  Uploaded to S3: s3://{AWS_S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"S3 upload error: {e}")

def cleanup_old_backups():
    """Remove backups older than retention period"""
    try:
        cutoff_date = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
        deleted_count = 0
        
        for backup_file in BACKUP_DIR.glob("*.sql"):
            if backup_file.stat().st_mtime < cutoff_date.timestamp():
                backup_file.unlink()
                # Delete encrypted version if exists
                encrypted = backup_file.with_suffix(backup_file.suffix + '.encrypted')
                if encrypted.exists():
                    encrypted.unlink()
                deleted_count += 1
        
        if deleted_count > 0:
            logger.info(f"🗑️  Cleaned up {deleted_count} old backup(s)")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def schedule_backups():
    """Schedule backup tasks"""
    if not BACKUP_ENABLED:
        logger.info("Backup scheduling is disabled")
        return
    
    if BACKUP_SCHEDULE == "daily":
        schedule.every().day.at(BACKUP_TIME).do(backup_database)
        logger.info(f"📅 Scheduled daily backup at {BACKUP_TIME}")
    elif BACKUP_SCHEDULE == "hourly":
        schedule.every().hour.do(backup_database)
        logger.info("📅 Scheduled hourly backups")
    elif BACKUP_SCHEDULE == "weekly":
        schedule.every().week.do(backup_database)
        logger.info("📅 Scheduled weekly backups")
    
    # Initial backup
    logger.info("Running initial backup...")
    backup_database()
    
    # Run scheduler
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    logger.info("🚀 Cloud Backup Service Starting...")
    schedule_backups()


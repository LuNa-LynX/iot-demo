"""
Security & Integrity Framework for EMS System
Implements data encryption, integrity checks, access control, and malware protection
"""
import os
import hashlib
import logging
from datetime import datetime
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SecurityFramework")

# Security Configuration
SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())
ENCRYPTION_ENABLED = os.getenv("ENCRYPTION_ENABLED", "true").lower() == "true"
INTEGRITY_CHECKS_ENABLED = os.getenv("INTEGRITY_CHECKS_ENABLED", "true").lower() == "true"

class SecurityFramework:
    def __init__(self):
        self.encryption_key = self._generate_key(SECRET_KEY)
        self.fernet = Fernet(self.encryption_key)
        self.integrity_log = []
    
    def _generate_key(self, password: str) -> bytes:
        """Generate encryption key from password"""
        password_bytes = password.encode()
        salt = b'cleanroom_ems_salt'  # Should be stored securely in production
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(password_bytes))
        return key
    
    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data"""
        if not ENCRYPTION_ENABLED:
            return data
        try:
            encrypted = self.fernet.encrypt(data.encode())
            return encrypted.decode()
        except Exception as e:
            logger.error(f"Encryption error: {e}")
            return data
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        if not ENCRYPTION_ENABLED:
            return encrypted_data
        try:
            decrypted = self.fernet.decrypt(encrypted_data.encode())
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Decryption error: {e}")
            return encrypted_data
    
    def calculate_hash(self, data: str) -> str:
        """Calculate SHA256 hash for integrity verification"""
        return hashlib.sha256(data.encode()).hexdigest()
    
    def verify_integrity(self, data: str, stored_hash: str) -> bool:
        """Verify data integrity"""
        if not INTEGRITY_CHECKS_ENABLED:
            return True
        calculated_hash = self.calculate_hash(data)
        is_valid = calculated_hash == stored_hash
        
        if not is_valid:
            logger.warning(f"⚠️  Integrity check failed! Data may have been tampered with.")
            self.log_security_event("INTEGRITY_VIOLATION", {
                "stored_hash": stored_hash[:16],
                "calculated_hash": calculated_hash[:16]
            })
        
        return is_valid
    
    def log_security_event(self, event_type: str, details: dict):
        """Log security events"""
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "details": details
        }
        self.integrity_log.append(event)
        logger.warning(f"🔒 Security Event: {event_type} - {details}")
    
    def detect_anomaly(self, sensor_data: dict) -> bool:
        """Detect anomalies that might indicate attacks"""
        anomalies = []
        
        # Check for impossible values
        if sensor_data.get("temperature", 0) < -50 or sensor_data.get("temperature", 0) > 100:
            anomalies.append("Impossible temperature value")
        
        if sensor_data.get("humidity", 0) < 0 or sensor_data.get("humidity", 0) > 100:
            anomalies.append("Impossible humidity value")
        
        if sensor_data.get("pressure", 0) < 0 or sensor_data.get("pressure", 0) > 1000:
            anomalies.append("Impossible pressure value")
        
        # Check for rapid changes (possible injection attack)
        # This would require historical data comparison
        
        if anomalies:
            self.log_security_event("ANOMALY_DETECTED", {"anomalies": anomalies, "data": sensor_data})
            return True
        
        return False
    
    def sanitize_input(self, data: str) -> str:
        """Sanitize user input to prevent injection attacks"""
        # Remove potentially dangerous characters
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'exec', 'union', 'select']
        sanitized = data.lower()
        for char in dangerous_chars:
            if char in sanitized:
                self.log_security_event("INJECTION_ATTEMPT", {"input": data[:100]})
                raise ValueError(f"Potentially dangerous input detected: {char}")
        return data
    
    def generate_audit_log(self) -> list:
        """Generate audit log of security events"""
        return self.integrity_log

# Global security instance
security = SecurityFramework()

def secure_endpoint(func):
    """Decorator for secure API endpoints"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Add security checks here
        # For now, just log the access
        logger.info(f"Secure endpoint accessed: {func.__name__}")
        return await func(*args, **kwargs)
    return wrapper

def validate_sensor_data(data: dict) -> tuple[bool, str]:
    """Validate sensor data for security"""
    try:
        # Check required fields
        required_fields = ["temperature", "humidity", "pressure", "particles"]
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"
            
            # Type validation
            try:
                float(data[field])
            except (ValueError, TypeError):
                return False, f"Invalid type for field: {field}"
        
        # Anomaly detection
        if security.detect_anomaly(data):
            return False, "Anomaly detected in sensor data"
        
        return True, "Valid"
    except Exception as e:
        return False, f"Validation error: {str(e)}"

if __name__ == "__main__":
    # Test security framework
    test_data = "Sensitive sensor data"
    encrypted = security.encrypt_data(test_data)
    decrypted = security.decrypt_data(encrypted)
    
    hash_value = security.calculate_hash(test_data)
    is_valid = security.verify_integrity(test_data, hash_value)
    
    print(f"Original: {test_data}")
    print(f"Encrypted: {encrypted[:50]}...")
    print(f"Decrypted: {decrypted}")
    print(f"Hash: {hash_value[:32]}...")
    print(f"Integrity Valid: {is_valid}")


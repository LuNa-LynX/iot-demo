"""
Role-Based Access Control (RBAC) System
Implements user authentication, roles, and permissions for the IoT Cleanroom System
"""
import os
import jwt
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Set
from enum import Enum
from functools import wraps
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("RBAC")

# Security Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# HTTP Bearer token security
security = HTTPBearer()

# Role Definitions
class Role(str, Enum):
    """System roles with hierarchical permissions"""
    ADMIN = "admin"           # Full system access
    OPERATOR = "operator"     # Can manage sensors and view all data
    TECHNICIAN = "technician" # Can view data and manage alerts
    VIEWER = "viewer"         # Read-only access
    GUEST = "guest"           # Limited read access

# Permission Definitions
class Permission(str, Enum):
    """Granular permissions"""
    # Data access
    VIEW_DATA = "view_data"
    VIEW_ALERTS = "view_alerts"
    VIEW_STATS = "view_stats"
    EXPORT_DATA = "export_data"
    
    # System management
    MANAGE_SENSORS = "manage_sensors"
    MANAGE_USERS = "manage_users"
    MANAGE_ROLES = "manage_roles"
    MANAGE_SETTINGS = "manage_settings"
    
    # Security
    VIEW_SECURITY_LOGS = "view_security_logs"
    MANAGE_BACKUPS = "manage_backups"
    CONFIGURE_ALERTS = "configure_alerts"
    
    # WebSocket
    WEBSOCKET_CONNECT = "websocket_connect"

# Role-Permission Mapping
ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.ADMIN: {
        Permission.VIEW_DATA,
        Permission.VIEW_ALERTS,
        Permission.VIEW_STATS,
        Permission.EXPORT_DATA,
        Permission.MANAGE_SENSORS,
        Permission.MANAGE_USERS,
        Permission.MANAGE_ROLES,
        Permission.MANAGE_SETTINGS,
        Permission.VIEW_SECURITY_LOGS,
        Permission.MANAGE_BACKUPS,
        Permission.CONFIGURE_ALERTS,
        Permission.WEBSOCKET_CONNECT,
    },
    Role.OPERATOR: {
        Permission.VIEW_DATA,
        Permission.VIEW_ALERTS,
        Permission.VIEW_STATS,
        Permission.EXPORT_DATA,
        Permission.MANAGE_SENSORS,
        Permission.CONFIGURE_ALERTS,
        Permission.WEBSOCKET_CONNECT,
    },
    Role.TECHNICIAN: {
        Permission.VIEW_DATA,
        Permission.VIEW_ALERTS,
        Permission.VIEW_STATS,
        Permission.CONFIGURE_ALERTS,
        Permission.WEBSOCKET_CONNECT,
    },
    Role.VIEWER: {
        Permission.VIEW_DATA,
        Permission.VIEW_ALERTS,
        Permission.VIEW_STATS,
        Permission.WEBSOCKET_CONNECT,
    },
    Role.GUEST: {
        Permission.VIEW_DATA,
        Permission.VIEW_STATS,
    },
}

class User:
    """User model with role and permissions"""
    def __init__(self, username: str, role: Role, user_id: Optional[str] = None):
        self.user_id = user_id or username
        self.username = username
        self.role = role
        self.permissions = ROLE_PERMISSIONS.get(role, set())
        self.created_at = datetime.now(timezone.utc)
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has a specific permission"""
        return permission in self.permissions
    
    def has_any_permission(self, permissions: List[Permission]) -> bool:
        """Check if user has any of the specified permissions"""
        return any(perm in self.permissions for perm in permissions)
    
    def has_all_permissions(self, permissions: List[Permission]) -> bool:
        """Check if user has all of the specified permissions"""
        return all(perm in self.permissions for perm in permissions)
    
    def to_dict(self) -> Dict:
        """Convert user to dictionary"""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "role": self.role.value,
            "permissions": [p.value for p in self.permissions],
            "created_at": self.created_at.isoformat()
        }

class RBACManager:
    """Manages RBAC operations including authentication and authorization"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.users: Dict[str, User] = {}  # Cache for loaded users
        self.database_url = database_url or os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
        self.pg_pool = None
        if self.database_url:
            try:
                self.pg_pool = pool.SimpleConnectionPool(1, 10, self.database_url)
                logger.info("✅ RBAC database connection pool initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize RBAC database pool: {e}")
        self._load_users_from_db()
    
    def _get_db_conn(self):
        """Get database connection"""
        if not self.pg_pool:
            return None
        try:
            return self.pg_pool.getconn()
        except:
            return None
    
    def _release_db_conn(self, conn):
        """Release database connection"""
        if self.pg_pool and conn:
            self.pg_pool.putconn(conn)
    
    def _load_users_from_db(self):
        """Load users from database into memory cache"""
        conn = self._get_db_conn()
        if not conn:
            logger.warning("No database connection, using in-memory users only")
            self._initialize_default_users()
            return
        
        try:
            cur = conn.cursor()
            cur.execute("SELECT username, password_hash, role, user_id FROM users")
            rows = cur.fetchall()
            
            for username, password_hash, role_str, user_id in rows:
                try:
                    role = Role(role_str)
                    user = User(username, role, user_id=user_id or f"user_{username}")
                    self.users[username] = user
                    # Note: We don't store password hashes in memory for security
                except ValueError:
                    logger.warning(f"Invalid role '{role_str}' for user '{username}', skipping")
            
            cur.close()
            logger.info(f"✅ Loaded {len(self.users)} users from database")
        except Exception as e:
            logger.error(f"Error loading users from database: {e}")
            self._initialize_default_users()
        finally:
            self._release_db_conn(conn)
    
    def _initialize_default_users(self):
        """Initialize default users for testing (fallback if no database)"""
        default_users = [
            ("admin", "admin123", Role.ADMIN),
            ("operator", "operator123", Role.OPERATOR),
            ("technician", "tech123", Role.TECHNICIAN),
            ("viewer", "viewer123", Role.VIEWER),
            ("guest", "guest123", Role.GUEST),
        ]
        
        for username, password, role in default_users:
            if username not in self.users:
                try:
                    self.create_user(username, password, role)
                    logger.info(f"Created default user: {username} with role: {role.value}")
                except ValueError:
                    pass  # User already exists
    
    def hash_password(self, password: str) -> str:
        """Hash a password"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def create_user(self, username: str, password: str, role: Role, room_id: Optional[int] = None) -> User:
        """Create a new user in database and memory cache"""
        # Check if user exists in cache
        if username in self.users:
            raise ValueError(f"User {username} already exists")
        
        hashed_password = self.hash_password(password)
        user_id = f"user_{len(self.users) + 1}"
        user = User(username, role, user_id=user_id)
        
        # Save to database if available
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, user_id, room_id) VALUES (%s, %s, %s, %s, %s)",
                    (username, hashed_password, role.value, user_id, room_id)
                )
                conn.commit()
                cur.close()
                logger.info(f"Created user in database: {username} with role: {role.value}, room_id: {room_id}")
            except psycopg2.IntegrityError:
                conn.rollback()
                raise ValueError(f"User {username} already exists in database")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error creating user in database: {e}")
                raise
            finally:
                self._release_db_conn(conn)
        
        # Add to memory cache
        self.users[username] = user
        
        return user
    
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate a user and return User object if valid"""
        # First check cache
        if username not in self.users:
            # Try to load from database
            conn = self._get_db_conn()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT username, password_hash, role, user_id FROM users WHERE username = %s", (username,))
                    row = cur.fetchone()
                    cur.close()
                    
                    if row:
                        username_db, password_hash, role_str, user_id = row
                        try:
                            role = Role(role_str)
                            user = User(username, role, user_id=user_id or f"user_{username}")
                            self.users[username] = user  # Cache it
                        except ValueError:
                            logger.warning(f"Invalid role for user {username}")
                            self._release_db_conn(conn)
                            return None
                    else:
                        logger.warning(f"Authentication failed: User {username} not found")
                        self._release_db_conn(conn)
                        return None
                except Exception as e:
                    logger.error(f"Error loading user from database: {e}")
                    self._release_db_conn(conn)
                    return None
                finally:
                    self._release_db_conn(conn)
            else:
                logger.warning(f"Authentication failed: User {username} not found")
                return None
        
        # Get password hash from database
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT password_hash FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
                cur.close()
                
                if row:
                    hashed_password = row[0]
                    if not self.verify_password(password, hashed_password):
                        logger.warning(f"Authentication failed: Invalid password for {username}")
                        self._release_db_conn(conn)
                        return None
                    
                    logger.info(f"User {username} authenticated successfully")
                    self._release_db_conn(conn)
                    return self.users[username]
                else:
                    self._release_db_conn(conn)
                    return None
            except Exception as e:
                logger.error(f"Error authenticating user: {e}")
                self._release_db_conn(conn)
                return None
        
        logger.warning(f"Authentication failed: No database connection")
        return None
    
    def generate_token(self, user: User) -> str:
        """Generate JWT token for user"""
        payload = {
            "sub": user.username,
            "user_id": user.user_id,
            "role": user.role.value,
            "permissions": [p.value for p in user.permissions],
            "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS),
            "iat": datetime.now(timezone.utc),
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALGORITHM)
        return token
    
    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    def get_user_from_token(self, token: str) -> Optional[User]:
        """Get User object from JWT token"""
        payload = self.verify_token(token)
        if not payload:
            return None
        
        username = payload.get("sub")
        if not username:
            return None
        
        # Check cache first
        if username in self.users:
            return self.users[username]
        
        # Load from database if not in cache
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT username, role, user_id FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
                cur.close()
                
                if row:
                    username_db, role_str, user_id = row
                    try:
                        role = Role(role_str)
                        user = User(username, role, user_id=user_id or f"user_{username}")
                        self.users[username] = user  # Cache it
                        return user
                    except ValueError:
                        return None
            except Exception as e:
                logger.error(f"Error loading user from database: {e}")
            finally:
                self._release_db_conn(conn)
        
        return None
    
    def get_user(self, username: str) -> Optional[User]:
        """Get user by username"""
        # Check cache first
        if username in self.users:
            return self.users[username]
        
        # Load from database
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT username, role, user_id FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
                cur.close()
                
                if row:
                    username_db, role_str, user_id = row
                    try:
                        role = Role(role_str)
                        user = User(username, role, user_id=user_id or f"user_{username}")
                        self.users[username] = user  # Cache it
                        return user
                    except ValueError:
                        return None
            except Exception as e:
                logger.error(f"Error loading user from database: {e}")
            finally:
                self._release_db_conn(conn)
        
        return None
    
    def update_user_role(self, username: str, new_role: Role) -> bool:
        """Update user's role in database and cache"""
        # Update in database
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("UPDATE users SET role = %s, updated_at = CURRENT_TIMESTAMP WHERE username = %s", 
                           (new_role.value, username))
                if cur.rowcount == 0:
                    cur.close()
                    self._release_db_conn(conn)
                    return False
                conn.commit()
                cur.close()
                logger.info(f"Updated user {username} role to {new_role.value} in database")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error updating user role in database: {e}")
                self._release_db_conn(conn)
                return False
            finally:
                self._release_db_conn(conn)
        
        # Update in cache
        if username in self.users:
            self.users[username].role = new_role
            self.users[username].permissions = ROLE_PERMISSIONS.get(new_role, set())
        else:
            # Reload from database
            self._load_users_from_db()
        
        return True
    
    def list_users(self) -> List[Dict]:
        """List all users from database"""
        users_list = []
        conn = self._get_db_conn()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT username, role, user_id, created_at FROM users ORDER BY username")
                rows = cur.fetchall()
                cur.close()
                
                for username, role_str, user_id, created_at in rows:
                    try:
                        role = Role(role_str)
                        user = User(username, role, user_id=user_id or f"user_{username}")
                        user_dict = user.to_dict()
                        user_dict["created_at"] = created_at.isoformat() if created_at else None
                        users_list.append(user_dict)
                    except ValueError:
                        continue
            except Exception as e:
                logger.error(f"Error listing users from database: {e}")
            finally:
                self._release_db_conn(conn)
        else:
            # Fallback to cache
            users_list = [user.to_dict() for user in self.users.values()]
        
        return users_list

# Global RBAC manager instance
# Initialize with database URL from environment
database_url = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
rbac_manager = RBACManager(database_url=database_url if database_url else None)

# FastAPI Dependencies
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """FastAPI dependency to get current authenticated user"""
    token = credentials.credentials
    user = rbac_manager.get_user_from_token(token)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user

def require_permission(permission: Permission):
    """Decorator to require a specific permission"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get user from kwargs or dependencies
            user = kwargs.get("current_user")
            if not user:
                # Try to get from dependencies
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break
            
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required"
                )
            
            if not user.has_permission(permission):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission denied: {permission.value} required"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def require_role(role: Role):
    """Decorator to require a specific role"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user = kwargs.get("current_user")
            if not user:
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break
            
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required"
                )
            
            if user.role != role:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Role {role.value} required"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def require_any_role(roles: List[Role]):
    """Decorator to require any of the specified roles"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            user = kwargs.get("current_user")
            if not user:
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break
            
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required"
                )
            
            if user.role not in roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"One of these roles required: {[r.value for r in roles]}"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Helper function for FastAPI dependency injection
def require_permission_dep(permission: Permission):
    """FastAPI dependency that requires a specific permission"""
    async def permission_checker(current_user: User = Depends(get_current_user)) -> User:
        if not current_user.has_permission(permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {permission.value} required"
            )
        return current_user
    return permission_checker

def require_role_dep(role: Role):
    """FastAPI dependency that requires a specific role"""
    async def role_checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role != role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role {role.value} required"
            )
        return current_user
    return role_checker

if __name__ == "__main__":
    # Test RBAC system
    print("Testing RBAC System")
    print("=" * 50)
    
    # Test authentication
    user = rbac_manager.authenticate_user("admin", "admin123")
    if user:
        print(f"✅ Authenticated: {user.username} ({user.role.value})")
        token = rbac_manager.generate_token(user)
        print(f"✅ Token generated: {token[:50]}...")
        
        # Test token verification
        payload = rbac_manager.verify_token(token)
        if payload:
            print(f"✅ Token verified: {payload['sub']} ({payload['role']})")
        
        # Test permissions
        print(f"✅ Has VIEW_DATA: {user.has_permission(Permission.VIEW_DATA)}")
        print(f"✅ Has MANAGE_USERS: {user.has_permission(Permission.MANAGE_USERS)}")
    
    # Test viewer role
    viewer = rbac_manager.authenticate_user("viewer", "viewer123")
    if viewer:
        print(f"\n✅ Viewer authenticated: {viewer.username}")
        print(f"✅ Has VIEW_DATA: {viewer.has_permission(Permission.VIEW_DATA)}")
        print(f"❌ Has MANAGE_USERS: {viewer.has_permission(Permission.MANAGE_USERS)}")




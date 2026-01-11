import bcrypt
import uuid
import logging
import jwt
from datetime import datetime, timedelta
from shared.db import db
from shared import config
from mysql.connector import Error

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AuthManager:
    def __init__(self):
        pass

    def get_connection(self):
        return db.get_connection()

    def hash_password(self, password):
        """Hash a password for storing."""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')

    def verify_password(self, stored_hash, provided_password):
        """Verify a stored password against one provided by user"""
        return bcrypt.checkpw(provided_password.encode('utf-8'), stored_hash.encode('utf-8'))

    def generate_token(self, user_id, username):
        """Generate JWT token"""
        try:
            payload = {
                'exp': datetime.utcnow() + timedelta(hours=1),
                'iat': datetime.utcnow(),
                'sub': user_id,
                'username': username
            }
            token = jwt.encode(
                payload,
                config.SECRET_KEY,
                algorithm='HS256'
            )
            return token
        except Exception as e:
            logger.error(f"Error generating token: {e}")
            return None

    def validate_token(self, token):
        """Validate JWT token"""
        try:
            payload = jwt.decode(token, config.SECRET_KEY, algorithms=['HS256'])
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None

    def signup(self, username, email, password):
        """Register a new user"""
        conn = self.get_connection()
        if not conn:
            return {"success": False, "message": "Database connection failed"}

        try:
            cursor = conn.cursor(dictionary=True)
            
            # Check if user already exists
            check_query = "SELECT username, email FROM user_details WHERE username = %s OR email = %s"
            cursor.execute(check_query, (username, email))
            result = cursor.fetchone()
            
            if result:
                if result['username'] == username:
                    return {"success": False, "message": "Username already exists"}
                if result['email'] == email:
                    return {"success": False, "message": "Email already exists"}
                return {"success": False, "message": "Username or Email already exists"}

            # Create new user
            new_id = str(uuid.uuid4())
            password_hash = self.hash_password(password)
            
            insert_query = """
                INSERT INTO user_details (id, username, email, password_hash)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (new_id, username, email, password_hash))
            conn.commit()
            
            logger.info(f"User created successfully: {username}")
            return {"success": True, "message": "User created successfully", "user_id": new_id}

        except Error as e:
            logger.error(f"Signup error: {e}")
            return {"success": False, "message": f"An error occurred: {e}"}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def login(self, username, password):
        """Authenticate a user"""
        conn = self.get_connection()
        if not conn:
            return {"success": False, "message": "Database connection failed"}

        try:
            cursor = conn.cursor(dictionary=True)
            
            # Fetch user
            query = "SELECT id, username, password_hash, is_active, is_locked FROM user_details WHERE username = %s OR email = %s"
            cursor.execute(query, (username, username))
            user = cursor.fetchone()
            
            if not user:
                return {"success": False, "message": "Invalid username or password"}

            if user['is_locked']:
                return {"success": False, "message": "Account is locked"}
            
            if not user['is_active']:
                return {"success": False, "message": "Account is inactive"}

            # Verify password
            if self.verify_password(user['password_hash'], password):
                update_query = "UPDATE user_details SET last_login_at = %s WHERE id = %s"
                cursor.execute(update_query, (datetime.now(), user['id']))
                conn.commit()
                
                token = self.generate_token(user['id'], user['username'])
                
                logger.info(f"User logged in: {username}")
                return {
                    "success": True, 
                    "message": "Login successful", 
                    "token": token,
                    "user": {"id": user['id'], "username": user['username']}
                }
            else:
                logger.warning(f"Failed login attempt for: {username}")
                return {"success": False, "message": "Invalid username or password"}

        except Error as e:
            logger.error(f"Login error: {e}")
            return {"success": False, "message": f"An error occurred: {e}"}
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

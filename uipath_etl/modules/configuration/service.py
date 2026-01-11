import logging
from shared.db import db
from mysql.connector import Error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigurationService:
    def __init__(self):
        pass

    def get_config(self):
        """Fetch the current UiPath configuration"""
        conn = db.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            # Fetch the latest configuration
            cursor.execute("SELECT base_url, client_id, client_secret, org, tenant, scope FROM uipath_configuration ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            return result
        except Error as e:
            logger.error(f"Error fetching config: {e}")
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()

    def update_config(self, data):
        """Update UiPath configuration (Insert new row to keep history)"""
        conn = db.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Helper to get value or default to current known value if partial update?
            # For simplicity, we assume full object or we fetch implementation details from caller.
            # But the requirement is "update". Let's assume a full payload or we need to merge.
            # For audit trail, let's INSERT the new config as the latest.
            
            insert_query = """
            INSERT INTO uipath_configuration 
            (base_url, client_id, client_secret, org, tenant, scope)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            val = (
                data.get('base_url'),
                data.get('client_id'),
                data.get('client_secret'),
                data.get('org'),
                data.get('tenant'),
                data.get('scope')
            )
            
            cursor.execute(insert_query, val)
            conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating config: {e}")
            return False
        finally:
            if conn and conn.is_connected():
                conn.close()

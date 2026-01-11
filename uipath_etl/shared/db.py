import mysql.connector
from mysql.connector import pooling, Error
import logging
import time
from . import config

logger = logging.getLogger(__name__)

class DatabasePool:
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance

    def _initialize_pool(self):
        db_config = {
            'host': config.MYSQL_HOST,
            'user': config.MYSQL_USER,
            'password': config.MYSQL_PASSWORD,
            'database': config.MYSQL_DATABASE,
            'pool_name': "uipath_pool",
            'pool_size': 10, # Adjustable
            'pool_reset_session': True
        }
        try:
            # Check/Wait for DB
            self._wait_for_db(db_config)
            
            logger.info("Initializing Database Connection Pool...")
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
            logger.info("Database Connection Pool created successfully.")
        except Error as e:
            logger.error(f"Error creating connection pool: {e}")
            raise

    def _wait_for_db(self, db_config, retries=5, delay=3):
        """Simple wait loop for DB to be ready"""
        # We need to test connection without pool parameters first
        test_config = {k:v for k,v in db_config.items() if k not in ['pool_name', 'pool_size', 'pool_reset_session']}
        for i in range(retries):
            try:
                conn = mysql.connector.connect(**test_config)
                if conn.is_connected():
                    conn.close()
                    return
            except Error:
                logger.warning(f"Database not ready, retrying {i+1}/{retries}...")
                time.sleep(delay)
        # If we fail here, the pool init will likely fail too, but let it throw correctly

    def get_connection(self):
        try:
            if self._pool:
                return self._pool.get_connection()
            else:
                self._initialize_pool()
                return self._pool.get_connection()
        except Error as e:
            logger.error(f"Error getting connection from pool: {e}")
            return None

# Global instance
db = DatabasePool()

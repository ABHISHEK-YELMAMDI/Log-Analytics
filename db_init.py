import mysql.connector
from mysql.connector import Error
import time
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'password')
}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    """Initialize the database and tables with retries"""
    retries = 5
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS log_analytics")
            logger.info("Database log_analytics created or already exists")
            conn.database = "log_analytics"

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                endpoint VARCHAR(255),
                timestamp FLOAT,
                timestamp_human DATETIME,
                response_time FLOAT,
                status INT
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                endpoint VARCHAR(255) PRIMARY KEY,
                request_count INT,
                avg_response_time FLOAT,
                timestamp DATETIME
            )
            """)
            logger.info("Database tables initialized successfully")
            conn.commit()
            conn.close()
            logger.info("Database initialization successful")
            return
        except Error as e:
            logger.warning(f"Attempt {attempt + 1}/{retries}: {e}, retrying in 5 seconds...")
            if attempt < retries - 1:
                time.sleep(5)
    raise Exception("Failed to initialize database")

if __name__ == "__main__":
    init_db()

import time
import mysql.connector
from mysql.connector import Error
import schedule
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'password'),
    'database': os.getenv('MYSQL_DATABASE', 'log_analytics')
}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database_connection():
    """Create a connection to the MySQL database with retries"""
    retries = 5
    for attempt in range(retries):
        try:
            connection = mysql.connector.connect(**DB_CONFIG)
            if connection.is_connected():
                logger.info(f"Connected to MySQL database: {DB_CONFIG['database']}")
                return connection
        except Error as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} - Error connecting to MySQL: {e}")
            if attempt < retries - 1:
                time.sleep(5)
    logger.error("Failed to connect to MySQL after retries")
    return None

def calculate_metrics():
    """Calculate and store metrics from request_logs"""
    connection = create_database_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()
        query = """
        SELECT endpoint, COUNT(*) as request_count, AVG(response_time) as avg_response_time
        FROM request_logs
        GROUP BY endpoint
        """
        cursor.execute(query)
        results = cursor.fetchall()

        for endpoint, count, avg_response in results:
            logger.info(f"Metric - Endpoint: {endpoint}, Request Count: {count}, Avg Response Time: {avg_response}")
            insert_query = """
            INSERT INTO metrics (endpoint, request_count, avg_response_time, timestamp)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                request_count = VALUES(request_count),
                avg_response_time = VALUES(avg_response_time),
                timestamp = NOW()
            """
            cursor.execute(insert_query, (endpoint, count, avg_response))
        connection.commit()
    except Error as e:
        logger.error(f"Error calculating metrics: {e}")
    finally:
        if connection.is_connected():
            connection.close()

if __name__ == "__main__":
    logger.info("Starting metrics generator scheduler")
    schedule.every(1).minutes.do(calculate_metrics)
    while True:
        schedule.run_pending()
        time.sleep(1)

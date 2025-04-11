import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'password'),
    'database': os.getenv('MYSQL_DATABASE', 'log_analytics')
}

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'request_logs')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'log_consumer_group')

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

def insert_log(connection, log_data):
    """Insert a log entry into the database"""
    try:
        cursor = connection.cursor()
        
        # Convert Unix timestamp to datetime
        timestamp_human = datetime.fromtimestamp(log_data['timestamp'])
        
        query = '''
        INSERT INTO request_logs 
        (endpoint, timestamp, timestamp_human, response_time, status) 
        VALUES (%s, %s, %s, %s, %s)
        '''
        
        values = (
            log_data['endpoint'],
            log_data['timestamp'],
            timestamp_human,
            log_data['response_time'],
            log_data['status']
        )
        
        cursor.execute(query, values)
        connection.commit()
        return True
    except Error as e:
        logger.error(f"Error inserting log: {e}")
        return False

def start_consumer():
    """Start the Kafka consumer to process logs"""
    connection = create_database_connection()
    if not connection:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    # Create Kafka consumer with retries
    retries = 5
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Starting Kafka consumer for topic: {KAFKA_TOPIC}")
            break
        except NoBrokersAvailable:
            logger.warning(f"Attempt {attempt + 1}/{retries}: Kafka not available, retrying in 5 seconds...")
            if attempt < retries - 1:
                time.sleep(5)
            else:
                logger.error("Failed to connect to Kafka after retries")
                return
    
    try:
        # Process messages
        for message in consumer:
            log_data = message.value
            logger.info(f"Received log: {log_data}")
            
            if insert_log(connection, log_data):
                logger.info(f"Successfully stored log for endpoint: {log_data['endpoint']}")
            else:
                logger.warning("Failed to store log")
            
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        start_consumer()
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"Error: {e}")

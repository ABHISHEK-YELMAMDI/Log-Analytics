from kafka import KafkaConsumer
import json
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka consumer configuration
consumer = KafkaConsumer(
    'access_logs', 'error_logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='log_consumer_group'
)

# PostgreSQL database setup
conn = psycopg2.connect(
    dbname="logs_db",
    user="postgres",
    password="b32d4f8c",  # Replace with your PostgreSQL password
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Create tables if they don’t exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS access_logs (
        request_id VARCHAR(36) PRIMARY KEY,
        timestamp TIMESTAMP,
        method VARCHAR(10),
        url TEXT,
        endpoint TEXT,
        status_code INTEGER,
        response_time REAL,
        user_agent TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS error_logs (
        request_id VARCHAR(36) PRIMARY KEY,
        timestamp TIMESTAMP,
        method VARCHAR(10),
        url TEXT,
        endpoint TEXT,
        status_code INTEGER,
        response_time REAL,
        user_agent TEXT,
        error TEXT
    )
''')
conn.commit()

# Process messages from Kafka
try:
    for message in consumer:
        log = message.value
        topic = message.topic
        logger.info(f"Consumed from {topic}: {log['request_id']}")

        if topic == 'access_logs':
            cursor.execute('''
                INSERT INTO access_logs 
                (request_id, timestamp, method, url, endpoint, status_code, response_time, user_agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    method = EXCLUDED.method,
                    url = EXCLUDED.url,
                    endpoint = EXCLUDED.endpoint,
                    status_code = EXCLUDED.status_code,
                    response_time = EXCLUDED.response_time,
                    user_agent = EXCLUDED.user_agent
            ''', (
                log['request_id'], log['timestamp'], log['method'], log['url'],
                log['endpoint'], log['status_code'], log['response_time'], log['user_agent']
            ))
        elif topic == 'error_logs':
            cursor.execute('''
                INSERT INTO error_logs 
                (request_id, timestamp, method, url, endpoint, status_code, response_time, user_agent, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    method = EXCLUDED.method,
                    url = EXCLUDED.url,
                    endpoint = EXCLUDED.endpoint,
                    status_code = EXCLUDED.status_code,
                    response_time = EXCLUDED.response_time,
                    user_agent = EXCLUDED.user_agent,
                    error = EXCLUDED.error
            ''', (
                log['request_id'], log['timestamp'], log['method'], log['url'],
                log['endpoint'], log['status_code'], log['response_time'], log['user_agent'], log['error']
            ))
        
        conn.commit()

except KeyboardInterrupt:
    logger.info("Consumer stopped by user")
finally:
    conn.close()
    consumer.close()
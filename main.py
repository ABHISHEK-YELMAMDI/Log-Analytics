from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import random
import json
import logging

app = FastAPI()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_producer():
    attempt = 1
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info("Kafka producer initialized successfully")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Attempt {attempt}: Kafka not available, retrying in 10 seconds...")
            time.sleep(10)
            attempt += 1

producer = init_producer()

def send_log(endpoint, status, response_time):
    log = {
        "endpoint": endpoint,
        "timestamp": time.time(),
        "response_time": response_time,
        "status": status
    }
    try:
        producer.send("request_logs", log)
        logger.info(f"Sent log to Kafka: {endpoint}")
    except Exception as e:
        logger.error(f"Error sending log to Kafka: {e}")

@app.get("/users")
def get_users():
    start_time = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    response_time = time.time() - start_time
    send_log("/users", 200, response_time)
    return {"message": "User list"}

@app.get("/products")
def get_products():
    start_time = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    response_time = time.time() - start_time
    send_log("/products", 200, response_time)
    return {"message": "Product list"}

@app.get("/orders")
def get_orders():
    start_time = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    response_time = time.time() - start_time
    send_log("/orders", 200, response_time)
    return {"message": "Order list"}

@app.get("/login")
def login():
    start_time = time.time()
    time.sleep(random.uniform(0.05, 0.2))
    response_time = time.time() - start_time
    send_log("/login", 200, response_time)
    return {"message": "Login successful"}

@app.get("/health")
def health():
    start_time = time.time()
    response_time = time.time() - start_time
    send_log("/health", 200, response_time)
    return {"status": "OK"}

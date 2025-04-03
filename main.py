from fastapi import FastAPI
from kafka import KafkaProducer
import time
import random
import json
from kafka.errors import NoBrokersAvailable

app = FastAPI()

def init_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers="kafka:9092",
                                     value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            print("Kafka producer initialized successfully")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {i+1}/{retries}: Kafka not available, retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka after multiple retries")

producer = init_producer()

def send_log(endpoint, status, response_time):
    log = {
        "endpoint": endpoint,
        "timestamp": time.time(),
        "response_time": response_time,
        "status": status
    }
    producer.send("request_logs", log)

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

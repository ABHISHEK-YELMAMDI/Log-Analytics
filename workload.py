import requests
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "http://localhost:8000"

def hit_endpoint(endpoint):
    try:
        response = requests.get(f"{API_URL}{endpoint}")
        logger.info(f"Hit {endpoint} - Status: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to hit {endpoint}: {e}")

if __name__ == "__main__":
    endpoints = ["/health", "/products", "/users", "/orders", "/login"]
    while True:
        for endpoint in endpoints:
            hit_endpoint(endpoint)
        time.sleep(1)

import requests
import random
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_URL = os.environ.get('API_URL', 'http://localhost:6969')
REQUEST_RATE = int(os.environ.get('REQUEST_RATE', 60))  # requests per minute

# Define endpoints with weights
ENDPOINTS = [
    {'path': '/', 'method': 'GET', 'weight': 5},
    {'path': '/users', 'method': 'GET', 'weight': 15},
    {'path': '/users/{id}', 'method': 'GET', 'weight': 10, 'params': {'id': lambda: random.randint(1, 15)}},
    {'path': '/products', 'method': 'GET', 'weight': 20},
    {'path': '/products/{id}', 'method': 'GET', 'weight': 15, 'params': {'id': lambda: random.randint(1, 15)}},
    {'path': '/orders', 'method': 'POST', 'weight': 10, 'data': lambda: {'product_id': random.randint(1, 10)}},
    {'path': '/orders/{id}', 'method': 'GET', 'weight': 10, 'params': {'id': lambda: 'error' if random.random() < 0.1 else str(random.randint(1000, 9999))}},
]

def select_endpoint():
    """Select a random endpoint based on weights"""
    total_weight = sum(endpoint['weight'] for endpoint in ENDPOINTS)
    r = random.uniform(0, total_weight)
    cumulative_weight = 0
    
    for endpoint in ENDPOINTS:
        cumulative_weight += endpoint['weight']
        if r <= cumulative_weight:
            return endpoint
    
    return ENDPOINTS[0]  # Fallback

def send_request():
    endpoint = select_endpoint()
    path = endpoint['path']
    method = endpoint['method']
    
    # Replace path parameters if any
    if '{id}' in path and 'params' in endpoint:
        for param_name, param_value_func in endpoint['params'].items():
            path = path.replace(f'{{{param_name}}}', str(param_value_func()))
    
    url = f"{API_URL}{path}"
    
    try:
        if method == 'GET':
            response = requests.get(url, timeout=5)
        elif method == 'POST':
            data = endpoint.get('data', lambda: {})()
            response = requests.post(url, json=data, timeout=5)
        
        logger.info(f"{method} {url} -> Status: {response.status_code}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")

def main():
    """Main function to generate load"""
    logger.info(f"Load generator started. Target API: {API_URL}")
    logger.info(f"Request rate: {REQUEST_RATE} requests per minute")
    
    # Calculate sleep time between requests
    sleep_time = 60 / REQUEST_RATE
    
    try:
        while True:
            send_request()
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Load generator stopped by user")

if __name__ == "__main__":
    main()


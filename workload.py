import requests
import random
import time

endpoints = ["users", "products", "orders", "login", "health"]
base_url = "http://localhost:8000"

while True:
    endpoint = random.choice(endpoints)
    try:
        response = requests.get(f"{base_url}/{endpoint}")
        print(f"Hit /{endpoint} - Status: {response.status_code}")
    except Exception as e:
        print(f"Error hitting /{endpoint}: {e}")
    time.sleep(random.uniform(0.1, 1))  # Random delay

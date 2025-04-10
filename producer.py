from flask import Flask, request, jsonify, g
import json
import time
import uuid
from datetime import datetime
import os
from kafka import KafkaProducer

app = Flask(__name__)

# Kafka producer configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all'
)

# Middleware for logging
@app.before_request
def before_request():
    g.request_id = str(uuid.uuid4())
    g.start_time = time.time()

@app.after_request
def after_request(response):
    response_time = (time.time() - g.start_time) * 1000  # in ms

    # Core log entry
    log_entry = {
        'request_id': g.request_id,
        'timestamp': datetime.utcnow().isoformat(),
        'method': request.method,
        'url': request.path,
        'endpoint': request.endpoint or 'unknown',
        'status_code': response.status_code,
        'response_time': response_time,
        'user_agent': request.headers.get('User-Agent', 'unknown')
    }

    # Send to Kafka
    try:
        producer.send('access_logs', log_entry)
        if response.status_code >= 400:
            error_log = {**log_entry, 'error': g.get('error', 'Unknown error')}
            producer.send('error_logs', error_log)
    except Exception as e:
        app.logger.error(f"Kafka send failed: {e}")

    return response

# Routes
@app.route('/')
def index():
    return jsonify({"message": "API is running"})

@app.route('/users')
def get_users():
    users = [{"id": i, "name": f"User {i}", "email": f"user{i}@example.com"} for i in range(1, 3)]
    return jsonify(users)

@app.route('/users/<int:user_id>')
def get_user(user_id):
    if user_id > 10:
        g.error = "User not found"
        return jsonify({"error": "User not found"}), 404
    return jsonify({"id": user_id, "name": f"User {user_id}", "email": f"user{user_id}@example.com"})

@app.route('/products')
def get_products():
    products = [{"id": i, "name": f"Product {i}", "price": i * 10.99} for i in range(1, 3)]
    return jsonify(products)

@app.route('/products/<int:product_id>')
def get_product(product_id):
    if product_id > 10:
        g.error = "Product not found"
        return jsonify({"error": "Product not found"}), 404
    return jsonify({"id": product_id, "name": f"Product {product_id}", "price": product_id * 5.99})

@app.route('/orders', methods=['POST'])
def create_order():
    data = request.json
    if not data or 'product_id' not in data:
        g.error = "Missing product_id"
        return jsonify({"error": "Missing product_id"}), 400
    order_id = str(uuid.uuid4())
    return jsonify({"order_id": order_id, "status": "created"}), 201

@app.route('/orders/<order_id>')
def get_order(order_id):
    if order_id == 'error':
        g.error = "Order not found"
        return jsonify({"error": "Order not found"}), 404
    return jsonify({"id": order_id, "status": "shipped"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6969)
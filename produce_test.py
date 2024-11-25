import json
import random
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "host.docker.internal:9094"  # Replace with your Kafka broker address
TOPIC = "test_topic"  # Replace with your Kafka topic

# Function to serialize messages as JSON
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    api_version=(0, 11, 5),
    value_serializer=json_serializer,
)

# Generate a random JSON object
def generate_random_json():
    return {
        "id": random.randint(1, 1000),
        "name": random.choice(["Alice", "Bob", "Charlie", "Diana"]),
        "age": random.randint(18, 65),
        "status": random.choice(["active", "inactive", "pending"]),
    }

# Send random JSON message
message = generate_random_json()

try:
    producer.send(TOPIC, value=message).get(timeout=10)
    print(f"Message sent successfully! Message: {message}")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
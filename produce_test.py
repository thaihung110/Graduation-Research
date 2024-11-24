from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Replace with your Kafka broker address
TOPIC = "test_topic"  # Replace with your Kafka topic

# Function to serialize messages
def serializer(message):
    return message.encode("utf-8")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0,11,5),
    value_serializer=serializer,
)
# Test message
message = "Hello, Kafka!"

try:
    producer.send('test_topic', value='test_message').get(timeout=10)
    print("Message sent successfully!")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()


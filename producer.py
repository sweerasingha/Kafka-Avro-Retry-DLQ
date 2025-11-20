import time
import random
import uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# 1. Load Schema
with open("order.avsc", "r") as f:
    schema_str = f.read()

# 2. Configure Schema Registry Client
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 3. Configure Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# 4. Configure Producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}
producer = SerializingProducer(producer_conf)

products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Produced: {msg.value()}")

print("Starting Producer... Press Ctrl+C to stop.")

try:
    while True:
        order = {
            "orderId": str(uuid.uuid4()),
            "product": random.choice(products),
            "price": round(random.uniform(10.0, 1000.0), 2)
        }

        # Produce message
        producer.produce(
            topic='orders', 
            key=str(order['orderId']), 
            value=order, 
            on_delivery=delivery_report
        )
        
        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")
    producer.flush()
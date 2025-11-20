from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

# --- CONFIGURATION ---
TOPIC_MAIN = "orders"
TOPIC_DLQ = "orders-dlq"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP_SERVERS = "localhost:9092"

# 1. Load Schema for DLQ Producer (Reusing same schema)
with open("order.avsc", "r") as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

# 2. Setup Deserializer (For consuming) and Serializer (For DLQ producing)
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
string_serializer = StringSerializer('utf_8')

# 3. Configure Consumer
consumer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'assignment_group_1',
    'auto.offset.reset': 'earliest'
}
consumer = DeserializingConsumer(consumer_conf)

# 4. Configure DLQ Producer
dlq_producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'key.serializer': string_serializer,
    'value.serializer': avro_serializer
}
dlq_producer = SerializingProducer(dlq_producer_conf)

# 5. State for Aggregation
stats = {} # Format: {'Product': {'total': 0.0, 'count': 0}}

def process_message(order):
    """
    Aggregates prices.
    SIMULATED FAILURE: If price > 900, we raise an error to test Retry/DLQ.
    """
    product = order['product']
    price = order['price']

    # This ensures we demonstrate the Retry and DLQ logic
    if price > 900:
        raise ValueError(f"Simulated processing error for high value item: {price}")
    # --------------------------------------------

    # Aggregation Logic
    if product not in stats:
        stats[product] = {'total': 0.0, 'count': 0}
    
    stats[product]['total'] += price
    stats[product]['count'] += 1
    
    avg = stats[product]['total'] / stats[product]['count']
    print(f"Processed {product} | Price: {price} | Running Avg: {avg:.2f}")

consumer.subscribe([TOPIC_MAIN])

print("Starting Consumer with Retry & DLQ... Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order_data = msg.value()
        order_key = msg.key()

        # --- RETRY LOGIC (Requirement: "Retry logic for temporary failures") ---
        max_retries = 3
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                process_message(order_data)
                success = True
            except Exception as e:
                retry_count += 1
                print(f"Error processing {order_key} (Attempt {retry_count}/{max_retries}): {e}")

        # --- DLQ LOGIC (Requirement: "DLQ for permanently failed messages") ---
        if not success:
            print(f"FAILED after retries. Sending to DLQ: {order_key}")
            dlq_producer.produce(
                topic=TOPIC_DLQ,
                key=order_key,
                value=order_data
            )
            dlq_producer.poll(0)

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    dlq_producer.flush()
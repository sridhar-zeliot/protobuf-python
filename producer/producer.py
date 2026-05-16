import random
import time
import os
import sys

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

sys.path.append("/app/generated")
import car_pb2

# -----------------------------
# ENV CONFIG
# -----------------------------
CAR_ID_MIN = int(os.getenv("CAR_ID_MIN", 1))
CAR_ID_MAX = int(os.getenv("CAR_ID_MAX", 15))
INTERVAL_MS = int(os.getenv("SCHEDULE_INTERVAL_MS", 10000))

# -----------------------------
# Schema Registry config
# -----------------------------
schema_registry_conf = {
    "url": "http://karapace-schema-registry.schema-registry:8081",
    "basic.auth.user.info": "ewaed6hp3rkggqk477ng4zas1:MrSV45FrMCa36Y7"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

protobuf_serializer = ProtobufSerializer(
    car_pb2.Car,
    schema_registry_client
)

# -----------------------------
# Kafka Producer config
# -----------------------------
producer_conf = {
    "bootstrap.servers": "my-cluster-kafka-bootstrap.kafka:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": protobuf_serializer,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "ewaed6hp3rkggqk477ng4zas1",
    "sasl.password": "MrSV45FrMCa36Y7"
}

producer = SerializingProducer(producer_conf)

topic = "car-nested-protobuf-topic"

# -----------------------------
# Delivery Callback
# -----------------------------
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

# -----------------------------
# Generate Car डेटा
# -----------------------------
def generate_car():
    car = car_pb2.Car()

    # carId from range
    car_id = random.randint(CAR_ID_MIN, CAR_ID_MAX)
    car.carId = str(car_id)

    # carName = KA07JB + last 2 digits
    car.carName = f"KA07JB{car_id:02d}"

    # Random values
    car.speed = random.uniform(40, 160)
    car.fuelLevel = random.randint(0, 100)
    car.headlight = random.choice([True, False])
    car.engineTemp = random.uniform(70, 110)

    car.location.latitude = random.uniform(-90, 90)
    car.location.longitude = random.uniform(-180, 180)

    return car

# -----------------------------
# Scheduler Loop
# -----------------------------
try:
    while True:
        car = generate_car()

        producer.produce(
            topic=topic,
            key=car.carId,
            value=car,
            on_delivery=delivery_report
        )

        # Important: non-blocking
        producer.poll(0)

        print(f"🚗 Sent: carId={car.carId}, carName={car.carName}")

        time.sleep(INTERVAL_MS / 1000)

except KeyboardInterrupt:
    print("🛑 Stopping producer...")

finally:
    producer.flush()
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer
import sys
sys.path.append("/app/generated")

import car_pb2

# -----------------------------
# Schema Registry config
# -----------------------------
schema_registry_conf = {
    "url": "http://karapace-schema-registry.schema-registry:8081",
    "basic.auth.user.info": "ewaed6hp3rkggqk477ng4zas1:MrSV45FrMCa36Y7"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Protobuf serializer
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
# Build Protobuf message
# -----------------------------
car = car_pb2.Car()
car.carId = "CAR-100"
car.carName = "Tesla Model X"
car.speed = 120.5
car.fuelLevel = 80
car.headlight = True
car.engineTemp = 75.2

car.location.latitude = 12.9716
car.location.longitude = 77.5946

# -----------------------------
# Produce
# -----------------------------
producer.produce(
    topic=topic,
    key=car.carId,
    value=car
)

producer.flush()

print("✅ Message sent successfully")
import os
import datetime
import json
import boto3
from kafka import KafkaProducer
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.adapter.kafka import KafkaSerializer
from aws_schema_registry.exception import SchemaRegistryException

def create_producer(bootstrap_servers: list, topic: str, registry: str, is_local: bool = False):
    glue_client = boto3.client("glue", region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"))
    client = SchemaRegistryClient(glue_client, registry_name=registry)
    serializer = KafkaSerializer(client)

    params = {
        "bootstrap_servers": bootstrap_servers,
        "key_serializer": lambda v: json.dumps(v, default=lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else obj).encode("utf-8"),
        "value_serializer": serializer,
    }
    if not is_local:
        params.update({"security_protocol": "SASL_SSL", "sasl_mechanism": "AWS_MSK_IAM"})
    return KafkaProducer(**params)

def send_messages(producer, topic, orders, schema):
    for order in orders:
        # try:
        producer.send(topic, key={"order_id": order["order_id"]}, value=(order, schema))
        # except SchemaRegistryException as e:
        #     raise RuntimeError("Failed to send a message") from e
    producer.flush()

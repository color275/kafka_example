import signal
import os
import json
import base64
import datetime
import boto3
from kafka import KafkaConsumer
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.adapter.kafka import KafkaDeserializer
from pprint import pprint
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()
v_bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
v_topic = os.getenv('TOPIC')
v_registry = os.getenv('REGISTRY')
v_auto_offset_reset = os.getenv('AUTO_OFFSET_RESET')

run = True
def signal_handler(signum, frame):
    global run
    print("Signal received, stopping consumer...")
    run = False

def create_deserializer(registry):
    glue_client = boto3.client("glue", region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"))
    client = SchemaRegistryClient(glue_client, registry_name=registry)
    return KafkaDeserializer(client)

def deserialize(deserializer, topic, bytes_):
    return deserializer.deserialize(topic, bytes_)

def parse_key(key):
    key_str = key.decode('utf-8')
    key_json = json.loads(key_str)
    return key_json

def lambda_handler(event, context):
    consumer = KafkaConsumer(
        v_topic,
        bootstrap_servers=v_bootstrap_servers.split(","),
        auto_offset_reset=v_auto_offset_reset,
        consumer_timeout_ms=1000    )

    deserializer = create_deserializer(v_registry)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while run:
        for message in consumer:
            if not run:  # 시그널에 의해 종료 요청이 들어오면 루프 종료
                break
            record = {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "timestampType": "CREATE_TIME",
                    "key": parse_key(message.key),
                    "value": deserialize(deserializer, message.topic, message.value).data,
                    "headers": message.headers
                }
            pprint(record)
    consumer.close()  

test_event = {}
test_context = {}

if __name__ == "__main__":
    response = lambda_handler(test_event, test_context)
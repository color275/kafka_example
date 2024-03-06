# https://jaehyeon.me/blog/2023-04-12-integrate-glue-schema-registry/

# error
# ModuleNotFoundError: No module named '_lzma'
# sudo yum install -y xz-devel
# cd Python-3.8.5
# sudo ./configure  --enable-optimizations --with-ssl
# sudo make
# sudo make install

# kafka for ui : http://3.35.137.48:8080/

import os
import datetime
import time
from aws_schema_registry.avro import AvroSchema
from src.order import create_orders
from src.producer import create_producer, send_messages
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()
v_bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
v_topic = os.getenv('TOPIC')
v_registry = os.getenv('REGISTRY')

def lambda_handler(event, context):
    bootstrap_servers = v_bootstrap_servers.split(",")
    topic = v_topic
    registry = v_registry
    producer = create_producer(bootstrap_servers, topic, registry, is_local=True)

    s = datetime.datetime.now()
    ttl_rec = 0
    while True:
        orders = create_orders(100)
        schema_dict = {
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "ordered_at", "type": "string"},  # ISO8601 형식의 날짜 문자열로 가정
                {"name": "user_id", "type": "string"},
                {
                    "name": "order_items",
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "OrderItem",
                            "fields": [
                                {"name": "product_id", "type": "int"},
                                {"name": "quantity", "type": "int"},
                            ],
                        },
                    },
                },
            ],
        }
        schema = AvroSchema(schema_dict)

        send_messages(producer, topic, orders, schema)
        ttl_rec += len(orders)
        print(f"Sent {len(orders)} messages")
        
        elapsed_sec = (datetime.datetime.now() - s).seconds
        if elapsed_sec > int(os.getenv("MAX_RUN_SEC", "60")):
            print(f"{ttl_rec} records sent in {elapsed_sec} seconds ...")
            break
        time.sleep(1)


test_event = {}
test_context = {}

if __name__ == "__main__":
    response = lambda_handler(test_event, test_context)
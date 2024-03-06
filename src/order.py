import datetime
import string
import json
from dataclasses import asdict, dataclass
from faker import Faker
from typing import List

faker = Faker()

# Removed enum Compatibility and class InjectCompatMixin for simplicity

def create_order_item(product_id: int, quantity: int):
    return {"product_id": product_id, "quantity": quantity}

def create_order(order_id: str, ordered_at: datetime.datetime, user_id: str, order_items: List[dict]):
    return {"order_id": order_id, "ordered_at": ordered_at, "user_id": user_id, "order_items": order_items}

def auto_create_order(fake: Faker = faker, num_items: int = 3):
    rand_int = fake.random_int(1, 1000)
    user_id = "".join([string.ascii_lowercase[int(s)] if s.isdigit() else s for s in hex(rand_int)])[::-1]
    order_items = [create_order_item(fake.random_int(1, 9999), fake.random_int(1, 10)) for _ in range(num_items)]
    # return create_order(fake.uuid4(), datetime.datetime.utcnow(), user_id, order_items)
    return create_order(fake.uuid4(), datetime.datetime.utcnow().isoformat(), user_id, order_items)

def create_orders(num: int):
    return [auto_create_order(num_items=3) for _ in range(num)]

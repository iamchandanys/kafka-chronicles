import json
import random
import uuid
import datetime

from confluent_kafka import Producer

class Orders:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': "localhost:9092"})

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed for order {msg.key()}: {err}")
        else:
            print(f"Order {msg.key()} produced to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    def produce_order(self, order_id, customer_id, product_id, quantity):
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'timestamp': datetime.datetime.now().isoformat()
        }
        self.producer.produce(
            topic='orders', 
            value=json.dumps(order).encode('utf-8'), 
            key=str(order_id),
            callback=self.delivery_report
        )
        self.producer.flush()


if __name__ == "__main__":
    orders = Orders()
    orders.produce_order(
        order_id=str(uuid.uuid4()), 
        customer_id=str(uuid.uuid4()), 
        product_id=str(uuid.uuid4()), 
        quantity=random.randint(1, 10)
    )
import json

from confluent_kafka import Consumer

class Inventory:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': "localhost:29092,localhost:39092,localhost:49092",
            'group.id': 'inventory-group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['orders'])

    def consume_orders(self):
        while True:
            msg = self.consumer.poll(20.0)
            if msg is None:
                print("No message received. Waiting...")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            value = msg.value()
            if value is None:
                print("Received message with no value. Skipping...")
                continue

            order = value.decode('utf-8')
            order_data = json.loads(order)
            print(
                f"Inventory updated for order: {order_data['order_id']}, "
                f"customer: {order_data['customer_id']}, "
                f"product: {order_data['product_id']}, "
                f"quantity: {order_data['quantity']}, "
                f"timestamp: {order_data['timestamp']}"
            )

if __name__ == "__main__":
    inventory = Inventory()
    inventory.consume_orders()
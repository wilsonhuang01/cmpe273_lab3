import logging
import json
import os
import threading

from flask import Flask, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


MESSAGES = {
    "created": "Order {order_id} created: {quantity} x {item_id}",
    "fulfilled": "Order {order_id} fulfilled",
    "cancelled": "Order {order_id} cancelled: {quantity} x {item_id} returned to inventory",
}

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "order-events")


def print_notification(payload):
    event = payload.get("event")
    order = payload.get("order") or {}
    if event not in MESSAGES or not isinstance(order, dict):
        logging.warning("invalid notification payload: %s", payload)
        return
    if not order.get("order_id") or not order.get("item_id") or not order.get("quantity"):
        logging.warning("notification missing order details: %s", payload)
        return

    message = MESSAGES[event].format(
        order_id=order["order_id"],
        item_id=order["item_id"],
        quantity=order["quantity"],
    )
    print(f"[NOTIFICATION] {message}", flush=True)


def consume_notifications():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="notification-service",
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    for message in consumer:
        print_notification(message.value)


@app.get("/health")
def health():
    return jsonify(status="ok", service="notification")


if __name__ == "__main__":
    threading.Thread(target=consume_notifications, daemon=True).start()
    app.run(host="127.0.0.1", port=5002, threaded=True)

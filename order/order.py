import logging
import json
import os
import threading
import time
import uuid
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://127.0.0.1:5001")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "order-events")

# The database is intentionally in memory for this lab.  The lock protects
# order state when Flask handles requests concurrently.
ORDERS = {}
ORDERS_LOCK = threading.Lock()


def send_notification(event, order):
    """Publish an order event to Kafka without failing the order operation."""
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            request_timeout_ms=600,
        )
        producer.send(
            KAFKA_TOPIC,
            {"event": event, "order": order},
            key=order["order_id"].encode("utf-8"),
        ).get(timeout=0.6)
        producer.close()
    except Exception as error:
        app.logger.warning(
            "kafka_notification_failed event=%s order_id=%s error=%s",
            event,
            order.get("order_id"),
            error,
        )


def fulfill_order(order_id):
    """Mark an unfulfilled order as fulfilled.

    Returns the updated order, or ``None`` when the order does not exist.
    A non-unfulfilled order is not changed and returns ``False``.
    """
    with ORDERS_LOCK:
        order = ORDERS.get(order_id)
        if order is None:
            return None
        if order["status"] != "unfulfilled":
            return False
        order["status"] = "fulfilled"
        result = dict(order)
    send_notification("fulfilled", result)
    return result


def cancel_order(order_id):
    """Cancel an unfulfilled order.

    Returns the updated order, or ``None`` when the order does not exist.
    A non-unfulfilled order is not changed and returns ``False``.
    """
    with ORDERS_LOCK:
        order = ORDERS.get(order_id)
        if order is None:
            return None
        if order["status"] != "unfulfilled":
            return False

        # Keep the lock while restoring stock so another request cannot
        # fulfill or cancel this order between the stock update and status
        # update.
        response = requests.post(
            f"{INVENTORY_URL}/v1/items/{order['item_id']}/add",
            json={"quantity": order["quantity"]},
            timeout=(0.2, 0.6),
            allow_redirects=False,
        )
        if response.status_code != 200:
            response.raise_for_status()
        order["status"] = "cancelled"
        result = dict(order)
    send_notification("cancelled", result)
    return result

@app.get("/health")
def health():
    return jsonify(status="ok", service="checkout")

@app.get("/v1/availability/<item_id>")
def availability(item_id):
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    status = 500
    try:
        with requests.get(
            f"{INVENTORY_URL}/v1/items/{item_id}",
            params={"delay_ms": request.args.get("delay_ms", "0")},
            headers={"X-Request-ID": request_id},
            timeout=(0.2, 0.6),
            allow_redirects=False,
        ) as response:
            if response.status_code == 404:
                body, status = {"error": "item_not_found"}, 404
            elif response.status_code == 400:
                body, status = {"error": "invalid_delay"}, 400
            elif response.status_code != 200:
                body, status = {"error": "dependency_bad_response"}, 502
            else:
                data = response.json()
                if (not isinstance(data, dict)
                    or data.get("item_id") != item_id
                    or type(data.get("available")) is not int
                    or data["available"] < 0):
                    raise ValueError("invalid inventory representation")
                body, status = data, 200
    except requests.Timeout:
        body, status = {"error": "dependency_timeout"}, 504
    except ValueError:
        body, status = {"error": "dependency_bad_response"}, 502
    except requests.RequestException:
        body, status = {"error": "dependency_unavailable"}, 503
    finally:
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        app.logger.info("request_id=%s status=%s elapsed_ms=%s",
                        request_id, status, elapsed_ms)
    
    result = jsonify(**body, request_id=request_id)
    result.status_code = status
    result.headers["X-Request-ID"] = request_id
    return result

@app.post("/v1/orders")
def create_order():
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    status = 500
    payload = request.get_json(silent=True) or {}
    item_id = payload.get("item_id")
    try:
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("invalid_item_id")
        quantity = int(payload.get("quantity"))
        if quantity < 1:
            raise ValueError("invalid_quantity")
    except (TypeError, ValueError):
        result = jsonify(error="invalid_order", request_id=request_id)
        result.status_code = 400
        result.headers["X-Request-ID"] = request_id
        return result
    try:
        with requests.post(
            f"{INVENTORY_URL}/v1/items/{item_id}/remove",
            json={"quantity": quantity},
            headers={"X-Request-ID": request_id},
            timeout=(0.2, 0.6),
            allow_redirects=False,
        ) as response:
            data = response.json() if response.content else {}
            if response.status_code == 404:
                body, status = {"error": "item_not_found"}, 404
            elif response.status_code == 400:
                body, status = {"error": "invalid_quantity"}, 400
            elif response.status_code == 409:
                body, status = {"error": "insufficient_stock"}, 409
            elif response.status_code != 200:
                body, status = {"error": "dependency_bad_response"}, 502
            else:
                remaining = data.get("remaining")
                if (not isinstance(data, dict)
                    or data.get("item_id") != item_id
                    or type(remaining) is not int
                    or remaining < 0):
                    raise ValueError("invalid inventory representation")
                order_id = str(uuid.uuid4())
                order = {
                    "order_id": order_id,
                    "item_id": item_id,
                    "quantity": quantity,
                    "status": "unfulfilled",
                }
                with ORDERS_LOCK:
                    ORDERS[order_id] = order
                send_notification("created", order)
                body, status = {
                    "order_id": order_id,
                    "status": "accepted",
                    "item_id": item_id,
                    "quantity": quantity,
                    "remaining": remaining,
                }, 201
    except requests.Timeout:
        body, status = {"error": "dependency_timeout"}, 504
    except ValueError:
        body, status = {"error": "dependency_bad_response"}, 502
    except requests.RequestException:
        body, status = {"error": "dependency_unavailable"}, 503
    finally:
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        app.logger.info("request_id=%s event=create_order status=%s elapsed_ms=%s",
                        request_id, status, elapsed_ms)

    result = jsonify(**body, request_id=request_id)
    result.status_code = status
    result.headers["X-Request-ID"] = request_id
    return result


def _order_transition_response(order_id, transition):
    """Apply an order transition and translate its result to an HTTP response."""
    order = transition(order_id)
    if order is None:
        result = jsonify(error="order_not_found", order_id=order_id)
        result.status_code = 404
    elif order is False:
        result = jsonify(error="invalid_order_status", order_id=order_id)
        result.status_code = 409
    else:
        result = jsonify(order)
        result.status_code = 200
    return result


@app.post("/v1/orders/<order_id>/fulfill")
def fulfill_order_endpoint(order_id):
    return _order_transition_response(order_id, fulfill_order)


@app.post("/v1/orders/<order_id>/cancel")
def cancel_order_endpoint(order_id):
    try:
        return _order_transition_response(order_id, cancel_order)
    except requests.Timeout:
        result = jsonify(error="inventory_dependency_timeout", order_id=order_id)
        result.status_code = 504
        return result
    except requests.RequestException:
        result = jsonify(error="inventory_dependency_unavailable", order_id=order_id)
        result.status_code = 503
        return result

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, threaded=True)

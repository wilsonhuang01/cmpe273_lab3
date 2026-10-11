import logging
import threading
import time
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

STOCK = {"sku-123": 7}
STOCK_LOCK = threading.Lock()

def is_quantity_available(item_id, quantity):
    stock = STOCK.get(item_id)
    if stock is None:
        return None
    if type(quantity) is not int or quantity < 1:
        raise ValueError("invalid_quantity")
    return stock >= quantity

def remove_quantity(item_id, quantity):
    if type(quantity) is not int or quantity < 1:
        raise ValueError("invalid_quantity")
    with STOCK_LOCK:
        stock = STOCK.get(item_id)
        if stock is None:
            return None
        if stock < quantity:
            return False
        STOCK[item_id] = stock - quantity
        return STOCK[item_id]

@app.get("/health")
def health():
    return jsonify(status="ok", service="inventory")

@app.get("/v1/items/<item_id>")
def get_item(item_id):
    request_id = request.headers.get("X-Request-ID", "none")
    try:
        delay_ms = int(request.args.get("delay_ms", "0"))
        if not 0 <= delay_ms <= 3000:
            raise ValueError()
    except ValueError:
        return jsonify(error="invalid_delay"), 400

    app.logger.info("request_id=%s event=started", request_id)
    time.sleep(delay_ms / 1000)
    app.logger.info("request_id=%s event=finished", request_id)

    stock = STOCK.get(item_id)
    if stock is None:
        return jsonify(error="item_not_found"), 404
    return jsonify(item_id=item_id, available=stock)

@app.post("/v1/items/<item_id>/remove")
def remove_item(item_id):
    payload = request.get_json(silent=True) or {}
    try:
        quantity = int(payload.get("quantity", ""))
        remaining = remove_quantity(item_id, quantity)
    except (TypeError, ValueError):
        return jsonify(error="invalid_quantity"), 400
    if remaining is None:
        return jsonify(error="item_not_found"), 404
    if remaining is False:
        return jsonify(error="insufficient_stock", item_id=item_id, stock=STOCK.get(item_id)), 409
    return jsonify(item_id=item_id, quantity=quantity, remaining=remaining)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, threaded=True)
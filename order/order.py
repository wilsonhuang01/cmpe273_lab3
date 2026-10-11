import logging
import os
import time
import uuid
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://127.0.0.1:5001")

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
                body, status = {
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

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, threaded=True)
# CMPE 273 Lab 3

## Architecture

This lab contains three services:

- Inventory: `http://127.0.0.1:5001`
- Order: `http://127.0.0.1:5000`
- Notification: `http://127.0.0.1:5002`

The Order service publishes order events to Kafka topic `order-events`. The
Notification service consumes those events and prints messages to its terminal.

![Architecture diagram](screenshots/1%20architecture.png)

Inventory manages stock for item IDs, including adding and removing stock on order_created and order_cancelled.
Order manages orders, it handles create_order, fulfill_order, cancel_order, as well as get_availability. It also publishes events to Kafka.
Notification acts as a consumer for Kafka, it prints message to terminal on new messages on Kafka.

Both Inventory and Order use in-memory dict as database, therefore data is reset when the service is restarted. Inventory manages stock for items, and Order manages orders with order_id.

I chose these service boundaries because it makes more sense. Inventory should not handle order creation and Order should not directly manage stock. Notification is a separate service because although it's currently only printing messages on terminal, in a future iteration, it could send email or SMS message. Separating it out from Inventory and Order makes them more loosely-coupled.

Interactions from Order to Inventory are Command and Query. Example Commands are create_order and cancel_order. Example Query is get_availability. Interactions from Order to Notification are Events, such as order_created and order_fulfilled.

## Start the services

### Start Kafka with Docker

Launch Docker Desktop and run Kafka locally on `127.0.0.1:9092`:

```bash
docker run -d --name cmpe273-kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:latest
```

Check that the container is running:

```bash
docker ps
docker logs cmpe273-kafka
```

Stop and remove it when finished:

```bash
docker stop cmpe273-kafka
docker rm cmpe273-kafka
```

Kafka must be listening on `127.0.0.1:9092` before starting the lab services.

### Start services

Install dependencies first if needed:

```bash
pip install -r inventory/requirements.txt
pip install -r notification/requirements.txt
pip install -r order/requirements.txt
```

Start services in separate terminals.

```bash
python3 inventory/inventory.py
python3 notification/notification.py
python3 order/order.py
```

The Kafka broker and topic can be changed with environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
export KAFKA_TOPIC=order-events
```

## Sample commands

Check service health:

```bash
curl http://127.0.0.1:5001/health
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5002/health
```

![Health](screenshots/2%20health.png)

Check item availability:

```bash
curl http://127.0.0.1:5000/v1/availability/sku-123
```

![Availability](screenshots/3%20availability.png)

Create an order. Save the returned `order_id` for the next commands:

```bash
curl -X POST http://127.0.0.1:5000/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"item_id":"sku-123","quantity":2}'
```

![Order](screenshots/4%20order.png)

The Notification terminal prints a message similar to:

```text
[NOTIFICATION] Order <order_id> created: 2 x sku-123
```

![Notification](screenshots/5%20notification.png)

Fulfill the order:

```bash
curl -X POST http://127.0.0.1:5000/v1/orders/<order_id>/fulfill
```

![Fulfill](screenshots/6%20fulfill.png)

Cancel an unfulfilled order. The cancelled quantity is added back to
inventory before the order is marked cancelled:

```bash
curl -X POST http://127.0.0.1:5000/v1/orders/<order_id>/cancel
```

![Order](screenshots/7%20order.png)
![Availbility](screenshots/8%20availability.png)
![Cancel](screenshots/9%20cancel.png)
![Availablity](screenshots/10%20availability.png)

Try to cancel an already fulfilled or cancelled order:

```bash
curl -i -X POST http://127.0.0.1:5000/v1/orders/<order_id>/cancel
```

![Invalid](screenshots/11%20invalid.png)

This returns `409 Conflict` because only unfulfilled orders can be cancelled.

![409](screenshots/12%20409.png)

## Failure test: Inventory service unavailable

1. Stop the Inventory service by switching to its terminal and pressing
   `Ctrl-C`.

2. Submit another order:

```bash
curl -i -X POST http://127.0.0.1:5000/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"item_id":"sku-123","quantity":1}'
```

The Order service should remain running and return an error similar to:

```text
HTTP/1.1 503 SERVICE UNAVAILABLE
{
  "error": "dependency_unavailable",
  "request_id": "<request_id>"
}
```

![Unavailable](screenshots/13%20unavailable.png)
![503](screenshots/14%20503.png)

The Order service itself should still respond to health checks:

```bash
curl http://127.0.0.1:5000/health
```

![Health](screenshots/15%20health.png)

Restart Inventory before continuing with normal order tests:

```bash
python3 lab3/inventory/inventory.py
```

The Inventory service also exposes the stock restoration operation directly:

```bash
curl -X POST http://127.0.0.1:5001/v1/items/sku-123/add \
  -H 'Content-Type: application/json' \
  -d '{"quantity":1}'
```

## Reflection

Clear service boundaries and data ownership are important in a distributed architecture because partial failure in services is common, a dependency may be down, an external call may be slow, and a database access can fail. A clear service boundary gives each service clear responsibilities, which makes them more fault tolerant when another service it depends on is unavailable. When working in a big organization, a clear data ownership is also important to give allow individual teams develop their services independently and reduce the chance of conflicts. Another advantage is the ease of debugging, when an error is found, it is easier to identify which service is the root cause.
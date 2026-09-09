import json
import os
import threading
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from flask import Flask, jsonify, redirect, render_template_string, request, url_for


app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "user-service-1")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})  # создание продюсера, который отправляет сообщение
sent_orders = [] # хранение последних заказов на странице
orders_lock = threading.Lock() # не позволяет нескольким потокам непраивльно изменить список

PAGE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>User service</title>
  <style>
    body { font-family: sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; }
    button { padding: 12px 18px; cursor: pointer; }
    table { border-collapse: collapse; width: 100%; margin-top: 24px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background: #f3f3f3; }
    code { background: #f3f3f3; padding: 2px 5px; }
  </style>
</head>
<body>
  <h1>Сервис пользователя</h1>
  <p>Экземпляр: <code>{{ instance_name }}</code></p>
  <p>Топик: <code>{{ topic }}</code></p>

  <form method="post" action="{{ url_for('create_order') }}">
    <button type="submit">Создать заказ</button>
  </form>

  <h2>Отправленные заказы</h2>
  <table>
    <thead>
      <tr><th>Order ID</th><th>Товары</th><th>Партиция</th><th>Offset</th></tr>
    </thead>
    <tbody>
      {% for order in orders %}
      <tr>
        <td>{{ order.order_id }}</td> 
        <td>{{ order["items"] | join(", ") }}</td>
        <td>{{ order.partition }}</td>
        <td>{{ order.offset }}</td>
      </tr>
      {% else %}
      <tr><td colspan="4">Заказов пока нет</td></tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""


@app.get("/")
def index():
    with orders_lock:
        orders = list(reversed(sent_orders))
    return render_template_string(
        PAGE,
        orders=orders,
        topic=KAFKA_TOPIC,
        instance_name=INSTANCE_NAME,
    )


@app.post("/orders")
def create_order(): # создание сообщения 
    order = {
        "order_id": str(uuid.uuid4())[:8],
        "items": ["кофе", "молоко"],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    delivery_finished = threading.Event() # ожидание ответа кафка
    delivery_result = {}

    def delivery_callback(error, message): # если получен результат отправки
        delivery_result["error"] = error
        if error is None:
            delivery_result["partition"] = message.partition()
            delivery_result["offset"] = message.offset()
        delivery_finished.set()

    producer.produce( # отправка сообщения
        topic=KAFKA_TOPIC,
        key=order["order_id"].encode("utf-8"),
        value=json.dumps(order, ensure_ascii=False).encode("utf-8"),
        callback=delivery_callback,
    )
    producer.flush(10) # результат отправки
    delivery_finished.wait(timeout=10)

    if not delivery_finished.is_set():
        return jsonify({"error": "Kafka did not confirm delivery in time"}), 504
    if delivery_result.get("error") is not None:
        return jsonify({"error": str(delivery_result["error"])}), 503

    order["partition"] = delivery_result["partition"]
    order["offset"] = delivery_result["offset"]
    with orders_lock:
        sent_orders.append(order)
        del sent_orders[:-20]

    return redirect(url_for("index"))


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

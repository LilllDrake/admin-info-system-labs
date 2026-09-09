import json
import os
import threading
import time

from confluent_kafka import Consumer, KafkaException
from flask import Flask, render_template_string


app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") # os.getenv - использование переменной окружения, иначе возьми дефолт - адрес кафки
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders") # читает сообщения из этого топика
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "pickers") # название группы consumers , кафка будет распредлять партиции между ними
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "picker-1") # имя конкретного сборщика
PROCESSING_SECONDS = float(os.getenv("PROCESSING_SECONDS", "2")) # количество секунд которое занимает сборка заказов

processed_orders = [] # список обработанных заказов
orders_lock = threading.Lock()
consumer_status = {"state": "starting", "error": None} # хранит текущее состояние

PAGE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="2">
  <title>Picker service</title>
  <style>
    body { font-family: sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; }
    table { border-collapse: collapse; width: 100%; margin-top: 24px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background: #f3f3f3; }
    code { background: #f3f3f3; padding: 2px 5px; }
  </style>
</head>
<body>
  <h1>Сервис сборщика</h1>
  <p>Экземпляр: <code>{{ instance_name }}</code></p>
  <p>Группа: <code>{{ group_id }}</code></p>
  <p>Топик: <code>{{ topic }}</code></p>
  <p>Состояние: <strong>{{ status.state }}</strong></p>
  {% if status.error %}<p>Последняя ошибка: {{ status.error }}</p>{% endif %}

  <h2>Обработанные заказы</h2>
  <table>
    <thead>
      <tr><th>Order ID</th><th>Товары</th><th>Партиция</th><th>Offset</th><th>Сборщик</th></tr>
    </thead>
    <tbody>
      {% for order in orders %}
      <tr>
        <td>{{ order.order_id }}</td>
        <td>{{ order["items"] | join(", ") }}</td>
        <td>{{ order.partition }}</td>
        <td>{{ order.offset }}</td>
        <td>{{ order.instance_name }}</td>
      </tr>
      {% else %}
      <tr><td colspan="5">Обработанных заказов пока нет</td></tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""


def consume_orders(): # создание consumer
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    ) 
    consumer.subscribe([KAFKA_TOPIC]) # подписывается на orders (топик) и только благодаря этому consumer может получать сообщения
    consumer_status["state"] = "waiting for orders"

    try: # бесконечная проверка сообщений, проверяет кафку
        while True: 
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            order = json.loads(message.value().decode("utf-8"))
            consumer_status["state"] = f"processing {order['order_id']}"
            time.sleep(PROCESSING_SECONDS)

            order["partition"] = message.partition()
            order["offset"] = message.offset()
            order["instance_name"] = INSTANCE_NAME

            with orders_lock:
                processed_orders.append(order)
                del processed_orders[:-50]

            consumer.commit(message=message, asynchronous=False)
            consumer_status["state"] = "waiting for orders"
            consumer_status["error"] = None
    except Exception as error:
        consumer_status["state"] = "error"
        consumer_status["error"] = str(error)
    finally:
        consumer.close()


@app.get("/")
def index():
    with orders_lock:
        orders = list(reversed(processed_orders))
    return render_template_string(
        PAGE,
        orders=orders,
        status=dict(consumer_status),
        topic=KAFKA_TOPIC,
        group_id=KAFKA_GROUP_ID,
        instance_name=INSTANCE_NAME,
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok", "consumer": consumer_status["state"]}


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_orders, daemon=True)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8001, threaded=True)

import os, time, json
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

EXCHANGE = os.getenv("EXCHANGE", "events.direct")
EXCHANGE_TYPE = os.getenv("EXCHANGE_TYPE", "direct")  # direct o topic

def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)

def ensure_exchange(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE, durable=True)

def publish_loop(producer_name: str, routing_key: str, event_type: str, interval_sec: float = 2.0):
    while True:
        try:
            conn = connect()
            ch = conn.channel()
            ensure_exchange(ch)

            while True:
                event = {
                    "producer": producer_name,
                    "type": event_type,
                    "id": __import__("uuid").uuid4().hex,
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "payload": {"msg": f"hello from {producer_name}", "n": int(time.time())},
                }

                body = json.dumps(event).encode("utf-8")
                ch.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                    ),
                )
                print(f"[{producer_name}] sent -> {routing_key} {event['id']}")
                time.sleep(interval_sec)

        except Exception as e:
            print(f"[{producer_name}] error: {e}. Reintentando en 3s...")
            time.sleep(3)

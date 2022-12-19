from os import environ
from redis import Redis
from uuid import uuid4

stream_key = "my-stream"
producer_name = "producer-service"
redis_host = "localhost"
redis_port = 6379
MAX_MESSAGES = 2

def connect():
    try:
        connection = Redis(redis_host, redis_port)
        connection.ping()
        return connection
    except Exception as e:
        raise Exception(f"ERROR:\t\tAn error occurred when connecting to Redis\n\t\t{e}")


def send_message(producer_id, message):
    try:
        message_payload = {
            "producer_id": producer_id,
            "message_id": uuid4().hex,
            "message_body": message
        }
    except Exception as e:
        raise Exception(f"ERROR:\t\tAn error occurred when sending the {i}-th message\n\t\t{e}")
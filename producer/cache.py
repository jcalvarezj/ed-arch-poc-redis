from os import environ
from redis import Redis
from uuid import uuid4

STREAM_KEY = "my-stream"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

class RedisConnection():
    def __init__(self):
        self._connect()

    def _connect(self):
        try:
            self.connection = Redis(REDIS_HOST, REDIS_PORT)
            self.connection.ping()
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when connecting to Redis\n\t\t{e}")

    def send_message(self, producer_id, message):
        try:
            message_payload = {
                "producer_id": producer_id,
                "message_id": uuid4().hex,
                "message_body": message
            }
            self.connection.xadd(STREAM_KEY, message_payload)
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when sending a message to {producer_id}\n\t\t{e}")
from os import environ
from redis import Redis
from uuid import uuid4

STREAM_KEY = "my-stream"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
LAST_MESSAGE_ID = 0
MAX_MESSAGES = 1

class RedisConnection():
    def __init__(self):
        self._connect()

    def _connect(self):
        try:
            self.connection = Redis(REDIS_HOST, REDIS_PORT)
            self.connection.ping()
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when connecting to Redis\n\t\t{e}")

    def pull_latest_message(self):
        try:
            response = self.connection.xread({STREAM_KEY: LAST_MESSAGE_ID}, MAX_MESSAGES, block=5000)
            return response
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when pulling messages from the message queue\n\t\t{e}")

    def decode_stream_data(self, raw_data):
        key, messages = raw_data[0]
        last_id, data = messages[0]

        data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
        data_dict["id"] = last_id.decode("utf-8")
        data_dict["key"] = key.decode("utf-8")
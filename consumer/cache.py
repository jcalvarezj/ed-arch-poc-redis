from os import environ
from redis import Redis
from uuid import uuid4

STREAM_KEY = "my-stream"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_MESSAGES = 1
BLOCKING_TIME_MS = 5000

class RedisConnection():
    last_message_id = 0

    def __init__(self):
        self._connect()

    def _connect(self):
        try:
            self.connection = Redis(REDIS_HOST, REDIS_PORT)
            self.connection.ping()
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when connecting to Redis\n\t\t{e}")

    def _decode_stream_data(self, raw_data):
        key, messages = raw_data[0]
        last_id, data = messages[0]

        data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
        data_dict["id"] = last_id.decode("utf-8")
        data_dict["key"] = key.decode("utf-8")

        return data_dict

    def pull_latest_message(self):
        try:
            response = self.connection.xread({STREAM_KEY: self.last_message_id}, MAX_MESSAGES, block=BLOCKING_TIME_MS)
            return self._decode_stream_data(response)
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when pulling messages from the message queue\n\t\t{e}")
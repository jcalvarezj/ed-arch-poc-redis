from os import environ
from redis import Redis
from uuid import uuid4

STREAM_KEY = "my-stream"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_MESSAGES = 1
BLOCKING_TIME_MS = 5000

class RedisConnection():
    """
    This class represents and contains the instance of a Redis connection
    """

    last_message_id = 0


    def __init__(self):
        """
        Constructor
        """
        self._connect()


    def _connect(self):
        """
        Attempts and stores connection to Redis
        """
        try:
            self.connection = Redis(REDIS_HOST, REDIS_PORT)
            self.connection.ping()
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when connecting to Redis\n\t\t{e}")


    def _decode_stream_data(self, raw_data):
        """
        Decodes the stream data into a dict that contains stream key, message id and payload
        """
        stream_key, messages = raw_data[0]
        last_id, data = messages[0]

        data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
        data_dict["id"] = last_id.decode("utf-8")
        data_dict["stream_key"] = stream_key.decode("utf-8")

        return data_dict


    def pull_latest_message(self):
        """
        Sequentially reads latest messages directly from the Stream
        """
        try:
            response = self.connection.xread({STREAM_KEY: self.last_message_id}, MAX_MESSAGES, block=BLOCKING_TIME_MS)
            if len(response) == 0:
                decoded_response = "No more data"
            else:
                decoded_response = self._decode_stream_data(response)
                self.last_message_id = decoded_response["id"]
            return decoded_response
        except Exception as e:
            raise Exception(f"ERROR:\t\tAn error occurred when pulling messages from the message queue\n\t\t{e}")


    def pop_latest_message(self):
        """
        Reads and acknowledges the latest message from the consumer group
        """
        pass

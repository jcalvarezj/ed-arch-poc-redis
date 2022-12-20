from os import environ
from redis import Redis
from redis.exceptions import ResponseError

STREAM_KEY = "my-stream"
GROUP_KEY = "my-group"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_MESSAGES = 1
GROUP_START_ID = "0" # Consume all pending messages in the Stream's history
GROUP_MESSAGE_ID = ">" # Next new message
BLOCKING_TIME_MS = 5000

class RedisConnection():
    """
    This class represents and contains the instance of a Redis connection
    """
    last_message_id = 0


    def __init__(self, service_name):
        """
        Constructor
        """
        self.service_name = service_name
        self._connect()


    def _connect(self):
        """
        Attempts and stores connection to Redis
        """
        try:
            self.connection = Redis(REDIS_HOST, REDIS_PORT)
            self.connection.ping()
        except Exception as e:
            raise Exception(f"ERROR: An error occurred when connecting to Redis\n\t{e}")


    def _decode_stream_data(self, raw_data):
        """
        Decodes the stream data into a dict that contains Stream key, message id and payload
        """
        stream_key, messages = raw_data[0]
        last_id, data = messages[0]

        data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
        data_dict["id"] = last_id.decode("utf-8")
        data_dict["stream_key"] = stream_key.decode("utf-8")

        return data_dict


    def _create_consumer_group(self):
        """
        Creates a consumer group in Redis for the Stream
        Given the current lack of features for Redis groups, errors for already existing groups are ignored
        Creates the Stream in case it does not exist
        """
        try:
            self.connection.xgroup_create(STREAM_KEY, GROUP_KEY, GROUP_START_ID, True)
        except ResponseError as rr:
            pass
        except Exception as e:
            print(f"ERROR: An error occurred when creating the consumer group\n\t{e}")


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
            raise Exception(f"ERROR: An error occurred when pulling messages from the message queue\n\t{e}")


    def pop_latest_message(self):
        """
        Reads and acknowledges the latest message from the consumer group
        """
        print("Creando")
        self._create_consumer_group()
        print("Creado")
        try:
            response = self.connection.xreadgroup(GROUP_KEY, self.service_name, {STREAM_KEY: GROUP_MESSAGE_ID},
                                                  MAX_MESSAGES, block=BLOCKING_TIME_MS)
            if len(response) == 0:
                decoded_response = "No more data"
            else:
                decoded_response = self._decode_stream_data(response)
                self.last_message_id = decoded_response["id"]
            return decoded_response
        except Exception as e:
            raise Exception(f"ERROR: An error occurred when popping messages from the consumer group\n\t{e}")


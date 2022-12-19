from fastapi import FastAPI
from fastapi import Query
from cache import RedisConnection

BLUEC = "\033[96m"
ENDC = "\033[0m"
PRODUCER_NAME = "producer-service"

redis_conn = None
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    try:
        print(f"{BLUEC}INFO:{ENDC}\t\tAttempting Redis connection")
        redis_conn = RedisConnection()
        print(f"{BLUEC}INFO:{ENDC}\t\tConnection to Redis OK!")
    except:
        raise Exception("Could not start the app!")

@app.get("/")
def home_handler():
    return {"message": "Hello world"}

@app.get("/produce")
def produce_handler():

    return {"message": f"Sent message to queue"}

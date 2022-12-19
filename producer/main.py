from datetime import datetime
from fastapi import FastAPI
from cache import RedisConnection


BLUEC = "\033[96m"
ENDC = "\033[0m"
PRODUCER_NAME = "producer-service"

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    try:
        print(f"{BLUEC}INFO:{ENDC}\t\tAttempting Redis connection")
        app.state.redis_conn = RedisConnection()
        print(f"{BLUEC}INFO:{ENDC}\t\tConnection to Redis OK!")
    except:
        raise Exception("Could not start the app!")


@app.get("/")
def home_handler():
    return {"message": "Hello world -- Use /produce [POST] to publish a new message to the Stream"}


@app.post("/produce")
def produce_handler():
    app.state.redis_conn.send_message(PRODUCER_NAME, f"Example message sent at {datetime.now()} !")
    return {"message": f"Sent message to queue"}

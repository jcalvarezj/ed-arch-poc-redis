from os import environ
from fastapi import FastAPI
from cache import RedisConnection


BLUEC = "\033[96m"
ENDC = "\033[0m"
SERVICE_NAME = environ.get("SERVICE_NAME", "consumer")

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
    message = app.state.redis_conn.pull_latest_message()
    return {"message": f"I am the {SERVICE_NAME} service. Last pulled message: {message}"}

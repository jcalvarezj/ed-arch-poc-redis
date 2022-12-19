from fastapi import FastAPI
from cache import connect

bluec = "\033[96m"
endc = "\033[0m"

redis_conn = None
app = FastAPI()

@app.on_event("startup")
async def startup_event():
    try:
        print(f"{bluec}INFO:{endc}\t\tAttempting Redis connection")
        redis_conn = connect()
        print(f"{bluec}INFO:{endc}\t\tConnection to Redis OK!")
    except:
        raise Exception("Could not start the app!")

@app.get("/produce")
def produce_handler():
    return {"message": "Hello world"}

# Producer

Producer service sub-project of the Event-Driven POC

This service generates and publishes messages on calls to the `/produce [POST]` endpoint, which are stacked in the **my-stream** Redis Stream

Run with `uvicorn main:app` after activating virtual environment (using venv with the `requirements.txt` dependencies file)
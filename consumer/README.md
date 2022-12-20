# Consumer

Consumer service sub-project of the Event-Driven POC

The root endpoint `/` displays the latest message from the **my-stream** Redis Stream

The `/pop` endpoint displays and acknowledges the lastest message, removing it from the **my-group** consumer group's PEL

Run with `uvicorn main:app --port <port number>` after activating virtual environment (using venv with the `requirements.txt` dependencies file)

Optionally, the `SERVICE_NAME` environment variable can be set to specify the consumer's name, for multiple consumer execution
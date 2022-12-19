# Producer

Consumer service sub-project of the Event-Driven POC

The root endpoint `/` pulls and displays the latest message from the **my-stream** Redis Stream

Run with `uvicorn main:app --port <port number>` after activating virtual environment (using venv with the `requirements.txt` dependencies file)
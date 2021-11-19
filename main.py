import json
import base64

from google.cloud import bigquery

from libs.gmail import get_gmail_service
from controller.email import run
from controller.watch import watch
from models.CallLogs import CallLogs

BQ_CLIENT = bigquery.Client()
DATASET = "CallTrackingMetrics"


def main(request) -> dict:
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "watch" in data:
        response = watch(get_gmail_service())
    elif "historyId" in data and "emailAddress" in data:
        response = run(
            BQ_CLIENT,
            get_gmail_service(),
            DATASET,
            CallLogs,
        )
    else:
        raise ValueError(data)

    print(response)
    return response

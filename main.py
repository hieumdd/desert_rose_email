import json
import base64

from emails import run
from watch import watch

def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "watch" in data:
        return watch()
    elif "historyId" in data and "emailAddress" in data:
        return run()
    else:
        raise ValueError(data)

import json
import base64
from unittest.mock import Mock

from main import main


def run(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    message = {"message": {"data": data_encoded}}
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    return res


def test_events():
    res = run(
        {
            "emailAddress": "siddhantmehandru.developer@gmail.com",
            "historyId": 2656967,
        }
    )
    if res["status"] == "Loaded":
        assert res["output_rows"] > 0
    else:
        assert res["message_id"]


def test_tasks():
    res = run(
        {
            "watch": 1,
        }
    )
    assert res["historyId"]

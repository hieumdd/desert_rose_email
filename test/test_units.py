import json
import base64
from unittest.mock import Mock

from main import main


def run(data):
    message = {"message": {"data": base64.b64encode(json.dumps(data).encode("utf-8"))}}
    return main(Mock(get_json=Mock(return_value=message), args=message))


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


def test_watch():
    res = run(
        {
            "watch": 1,
        }
    )
    assert res["historyId"]

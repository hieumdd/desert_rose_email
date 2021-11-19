import base64
import csv
import re

import requests
from google.cloud import bigquery
from googleapiclient.discovery import Resource

from libs.gmail import get_latest_email_id, get_message
from libs.bigquery import is_loaded, load
from models.CallLogs import CallLogs


def get_message_html(message: dict) -> str:
    return [
        part for part in message["payload"]["parts"] if part["mimeType"] == "text/html"
    ][0]["body"]["data"]


def parse_message(message: str) -> str:
    return base64.urlsafe_b64decode(message.encode()).decode()


def extract_csv_link(html: str) -> str:
    search = re.search('href="?([^\'" >]+\.csv)"', html)
    if search:
        return search.group(1)
    else:
        raise ValueError(html)


def get_csv(url: str) -> str:
    with requests.get(url) as r:
        return r.content.decode()


def convert_to_json(data: str) -> list[dict]:
    lines = data.splitlines()
    return [
        row
        for row in csv.DictReader(
            lines[1:],
            fieldnames=[i.replace('"', "") for i in lines[0].split(",")],
        )
    ]


def run(
    client: bigquery.Client,
    gmail: Resource,
    dataset: str,
    model: dict = CallLogs,
) -> dict:
    message_id = get_latest_email_id(gmail)
    if is_loaded(client, dataset, model["meta_table"], message_id) == 0:
        response = {
            "status": "Loaded",
            "output_rows": load(
                client,
                dataset,
                model["name"],
                model["schema"],
                model["transform"](
                    convert_to_json(
                        get_csv(
                            extract_csv_link(
                                parse_message(
                                    get_message_html(get_message(gmail, message_id))
                                )
                            )
                        )
                    )
                ),
            ),
        }
        load(
            client,
            dataset,
            model["meta_table"],
            model["meta_schema"],
            [{"message_id": message_id}],
        )
    else:
        response = {
            "status": "Skipped",
            "message_id": message_id,
        }
    return response

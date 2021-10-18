import base64
import csv
import re

import requests
from google.cloud import bigquery

from auth import get_gmail_service

BQ_CLIENT = bigquery.Client()

SCHEMA = [
    {"name": "date", "type": "DATE"},
    {"name": "call_id", "type": "NUMERIC"},
    {"name": "customer_no", "type": "STRING"},
    {"name": "source", "type": "STRING"},
    {"name": "medium", "type": "STRING"},
    {"name": "campaign_id", "type": "STRING"},
    {"name": "ad_group_id", "type": "STRING"},
    {"name": "adgroup_id", "type": "STRING"},
    {"name": "keyword", "type": "STRING"},
    {"name": "duration", "type": "NUMERIC"},
    {"name": "tags", "type": "STRING"},
]


def get_latest_email_id(gmail):
    return (
        gmail.users()
        .messages()
        .list(
            userId="me",
            maxResults=1,
            q='from:no-reply@calltrackingmetrics.com "d30c40cb56c05959452b08ffcbeb78a7"',
        )
        .execute()["messages"][0]["id"]
    )


def get_message(gmail, message_id):
    return (
        gmail.users()
        .messages()
        .get(
            userId="me",
            format="full",
            id=message_id,
        )
        .execute()
    )


def get_message_html(message):
    return [
        part for part in message["payload"]["parts"] if part["mimeType"] == "text/html"
    ][0]["body"]["data"]


def parse_message(message):
    return base64.urlsafe_b64decode(message.encode()).decode()


def extract_csv_link(html):
    return re.search('href="?([^\'" >]+\.csv)"', html).group(1)


def get_csv(url):
    with requests.get(url) as r:
        return r.content.decode()


def convert_to_json(data):
    lines = data.splitlines()
    return [
        row
        for row in csv.DictReader(
            lines[1:],
            fieldnames=[i.replace('"', "") for i in lines[0].split(",")],
        )
    ]


def transform(rows):
    return [
        {
            "date": row["Date"],
            "call_id": int(row["CallId"]),
            "customer_no": row["Customer #"],
            "source": row["source"],
            "medium": row["medium"],
            "campaign_id": row["campaign_id"],
            "ad_group_id": row["ad_group_id"],
            "adgroup_id": row["adgroup_id"],
            "keyword": row["keyword"],
            "duration": int(row["Duration"]),
            "tags": row["Tags"],
        }
        for row in rows
    ]


def load(rows):
    return (
        BQ_CLIENT.load_table_from_json(
            rows,
            "CallTrackingMetrics.CallLogs",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=SCHEMA,
            ),
        )
        .result()
        .output_rows
    )


def run():
    gmail = get_gmail_service()
    return {
        "output_rows": load(
            transform(
                convert_to_json(
                    get_csv(
                        extract_csv_link(
                            parse_message(
                                get_message_html(
                                    get_message(gmail, get_latest_email_id(gmail))
                                )
                            )
                        )
                    )
                )
            )
        ),
    }


x = run()
x

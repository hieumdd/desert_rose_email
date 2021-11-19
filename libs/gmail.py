import os

import httplib2
from oauth2client import client
from googleapiclient.discovery import Resource, build


def get_gmail_service() -> Resource:
    cred = client.GoogleCredentials(
        access_token=None,
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        refresh_token=os.getenv("REFRESH_TOKEN"),
        token_expiry=None,
        token_uri="https://accounts.google.com/o/oauth2/token",
        user_agent="",
    )
    http = cred.authorize(httplib2.Http())
    cred.refresh(http)
    return build("gmail", "v1", credentials=cred)


def get_latest_email_id(gmail: Resource) -> str:
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


def get_message(gmail: Resource, message_id: str) -> dict:
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


def watch():
    def watch_exec():
        return (
            gmail.users()
            .watch(
                userId="me",
                body={
                    "topicName": "projects/desert-rose-325315/topics/gmail-watch",
                },
            )
            .execute()
        )

    gmail = get_gmail_service()
    try:
        return watch_exec()
    except Exception as e:
        print(e)
        gmail.users().stop(userId="me").execute()
        return watch_exec()

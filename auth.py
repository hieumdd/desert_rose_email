import os

import httplib2
from oauth2client import client
from googleapiclient.discovery import build


def get_gmail_service():
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

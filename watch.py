from auth import get_gmail_service


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

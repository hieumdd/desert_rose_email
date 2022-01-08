safe_int = lambda x: int(x) if x else None

CallLogs = {
    "name": "CallLogs",
    "transform": lambda rows: [
        {
            "date": row["Date"],
            "call_id": safe_int(row["CallId"]),
            "customer_no": row["Customer #"],
            "source": row["source"],
            "medium": row["medium"],
            "campaign_id": row["campaign_id"],
            "ad_group_id": row["ad_group_id"],
            "adgroup_id": row["adgroup_id"],
            "keyword": row["keyword"],
            "duration": safe_int(row["Duration"]),
            "tags": row["Tags"],
        }
        for row in rows
    ],
    "schema": [
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
    ],
    "meta_table": "meta_messages",
    "meta_schema": [{"name": "message_id", "type": "STRING"}],
}

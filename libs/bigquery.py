from google.cloud import bigquery


def is_loaded(
    client: bigquery.Client,
    dataset: str,
    table: str,
    message_id: str,
) -> int:
    return [
        dict(row.items())
        for row in client.query(
            f"""
            SELECT COUNT(*) AS found FROM {dataset}.{table}
            WHERE message_id = "{message_id}"
            """
        ).result()
    ][0]["found"]


def load(
    client: bigquery.Client,
    dataset: str,
    table: str,
    schema: list[dict],
    rows: list[dict],
) -> int:
    return (
        client.load_table_from_json(
            rows,
            f"{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema,
            ),
        )
        .result()
        .output_rows
    )

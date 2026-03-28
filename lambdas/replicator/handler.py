import os
import json
from datetime import UTC, datetime
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Attr, Key


s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

DEST_BUCKET_NAME = os.environ["DEST_BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
MAX_ACTIVE_COPIES = int(os.environ.get("MAX_ACTIVE_COPIES", "3"))

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, _context):
    results = []

    for record in iter_s3_records(event):
        event_name = record.get("eventName", "")

        if event_name.startswith("ObjectCreated:Put"):
            results.append(handle_put(record))
        elif event_name.startswith("ObjectRemoved:Delete"):
            results.append(handle_delete(record))

    return {
        "processed_records": len(results),
        "results": results,
    }


def iter_s3_records(event):
    for record in event.get("Records", []):
        if record.get("eventSource") == "aws:sqs":
            body = json.loads(record["body"])
            for nested_record in body.get("Records", []):
                yield nested_record
        else:
            yield record


def handle_put(record):
    source_bucket = record["s3"]["bucket"]["name"]
    source_key = decode_s3_key(record["s3"]["object"]["key"])
    created_at_ms = now_ms()
    copy_key = build_copy_key(source_key, created_at_ms)

    s3_client.copy_object(
        Bucket=DEST_BUCKET_NAME,
        Key=copy_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )

    new_item = {
        "original_key": source_key,
        "copy_key": copy_key,
        "created_at": created_at_ms,
        "status": "ACTIVE",
    }

    table.put_item(Item=new_item)

    query_response = table.query(
        KeyConditionExpression=Key("original_key").eq(source_key),
        ConsistentRead=True,
    )
    active_items = sorted(
        [item for item in query_response["Items"] if item.get("status") == "ACTIVE"],
        key=lambda item: item["created_at"],
    )

    deleted_oldest = None
    if len(active_items) > MAX_ACTIVE_COPIES:
        oldest_item = active_items[0]
        s3_client.delete_object(Bucket=DEST_BUCKET_NAME, Key=oldest_item["copy_key"])
        table.delete_item(
            Key={
                "original_key": oldest_item["original_key"],
                "copy_key": oldest_item["copy_key"],
            }
        )
        deleted_oldest = oldest_item["copy_key"]

    return {
        "action": "PUT",
        "source_key": source_key,
        "new_copy_key": copy_key,
        "deleted_oldest_copy_key": deleted_oldest,
    }


def handle_delete(record):
    source_key = decode_s3_key(record["s3"]["object"]["key"])
    disowned_at = now_ms()

    query_response = table.query(
        KeyConditionExpression=Key("original_key").eq(source_key),
        ConsistentRead=True,
        FilterExpression=Attr("status").eq("ACTIVE"),
    )
    active_items = query_response["Items"]

    for item in active_items:
        table.update_item(
            Key={
                "original_key": item["original_key"],
                "copy_key": item["copy_key"],
            },
            UpdateExpression=(
                "SET #status = :status, "
                "disowned_at = :disowned_at"
            ),
            ExpressionAttributeNames={
                "#status": "status",
            },
            ExpressionAttributeValues={
                ":status": "DISOWNED",
                ":disowned_at": disowned_at,
            },
        )

    return {
        "action": "DELETE",
        "source_key": source_key,
        "disowned_copies": len(active_items),
    }


def decode_s3_key(raw_key):
    return unquote_plus(raw_key)


def build_copy_key(source_key, created_at_ms):
    return f"{source_key}#{created_at_ms:013d}"


def now_ms():
    return int(datetime.now(UTC).timestamp() * 1000)

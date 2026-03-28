import os
from datetime import UTC, datetime

import boto3
from boto3.dynamodb.conditions import Key


s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

DEST_BUCKET_NAME = os.environ["DEST_BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]
DISOWNED_INDEX_NAME = os.environ["DISOWNED_INDEX_NAME"]
DISOWNED_AGE_SECONDS = int(os.environ.get("DISOWNED_AGE_SECONDS", "10"))

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(_event, _context):
    cutoff_ms = now_ms() - (DISOWNED_AGE_SECONDS * 1000)
    deleted = []
    last_evaluated_key = None

    while True:
        query_kwargs = {
            "IndexName": DISOWNED_INDEX_NAME,
            "KeyConditionExpression": Key("status").eq("DISOWNED")
            & Key("disowned_at").lte(cutoff_ms),
        }
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**query_kwargs)

        for item in response.get("Items", []):
            s3_client.delete_object(Bucket=DEST_BUCKET_NAME, Key=item["copy_key"])
            table.delete_item(
                Key={
                    "original_key": item["original_key"],
                    "copy_key": item["copy_key"],
                }
            )
            deleted.append(item["copy_key"])

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return {
        "deleted_count": len(deleted),
        "deleted_copy_keys": deleted,
        "cutoff_ms": cutoff_ms,
    }


def now_ms():
    return int(datetime.now(UTC).timestamp() * 1000)

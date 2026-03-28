from pathlib import Path

from aws_cdk import Duration, Stack, aws_lambda as lambda_, aws_s3 as s3, aws_sqs as sqs
from aws_cdk import aws_lambda_event_sources as event_sources
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        event_queue: sqs.IQueue,
        source_bucket: s3.IBucket,
        destination_bucket: s3.IBucket,
        table,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler_path = Path(__file__).resolve().parent.parent / "lambdas" / "replicator"

        replicator_fn = lambda_.Function(
            self,
            "ReplicatorFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(str(handler_path)),
            timeout=Duration.seconds(30),
            environment={
                "DEST_BUCKET_NAME": destination_bucket.bucket_name,
                "TABLE_NAME": table.table_name,
                "MAX_ACTIVE_COPIES": "3",
            },
        )

        source_bucket.grant_read(replicator_fn)
        destination_bucket.grant_read_write(replicator_fn)
        event_queue.grant_consume_messages(replicator_fn)
        table.grant_read_write_data(replicator_fn)

        replicator_fn.add_event_source(
            event_sources.SqsEventSource(
                event_queue,
                batch_size=10,
            )
        )

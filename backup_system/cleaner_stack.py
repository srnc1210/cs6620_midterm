from pathlib import Path

from aws_cdk import Duration, Stack, aws_events as events, aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_, aws_s3 as s3
from constructs import Construct


class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        destination_bucket: s3.IBucket,
        table,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        handler_path = Path(__file__).resolve().parent.parent / "lambdas" / "cleaner"

        cleaner_fn = lambda_.Function(
            self,
            "CleanerFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(str(handler_path)),
            timeout=Duration.seconds(30),
            environment={
                "DEST_BUCKET_NAME": destination_bucket.bucket_name,
                "TABLE_NAME": table.table_name,
                "DISOWNED_INDEX_NAME": "StatusIndex",
                "DISOWNED_AGE_SECONDS": "10",
            },
        )

        destination_bucket.grant_read_write(cleaner_fn)
        table.grant_read_write_data(cleaner_fn)

        schedule_rule = events.Rule(
            self,
            "CleanerScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        schedule_rule.add_target(targets.LambdaFunction(cleaner_fn))

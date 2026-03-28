from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.source_bucket = s3.Bucket(
            self,
            "SourceBucket",
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=False,
            removal_policy=RemovalPolicy.DESTROY,
            versioned=False,
        )

        self.destination_bucket = s3.Bucket(
            self,
            "DestinationBucket",
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=False,
            lifecycle_rules=[
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
            versioned=False,
        )

        self.replicator_queue = sqs.Queue(
            self,
            "ReplicatorQueue",
            visibility_timeout=Duration.seconds(60),
        )

        replicator_queue_policy = sqs.QueuePolicy(
            self,
            "ReplicatorQueuePolicy",
            queues=[self.replicator_queue],
        )
        replicator_queue_policy.document.add_statements(
            iam.PolicyStatement(
                actions=[
                    "sqs:SendMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                ],
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                resources=[self.replicator_queue.queue_arn],
                conditions={
                    "StringEquals": {
                        "aws:SourceAccount": self.account,
                    }
                },
            )
        )

        self.table = dynamodb.Table(
            self,
            "BackupTable",
            partition_key=dynamodb.Attribute(
                name="original_key",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="copy_key",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.table.add_global_secondary_index(
            index_name="StatusIndex",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="disowned_at",
                type=dynamodb.AttributeType.NUMBER,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        source_bucket_resource = self.source_bucket.node.default_child
        source_bucket_resource.notification_configuration = s3.CfnBucket.NotificationConfigurationProperty(
            queue_configurations=[
                s3.CfnBucket.QueueConfigurationProperty(
                    event="s3:ObjectCreated:Put",
                    queue=self.replicator_queue.queue_arn,
                ),
                s3.CfnBucket.QueueConfigurationProperty(
                    event="s3:ObjectRemoved:Delete",
                    queue=self.replicator_queue.queue_arn,
                ),
            ]
        )
        source_bucket_resource.add_dependency(replicator_queue_policy.node.default_child)

        CfnOutput(
            self,
            "SourceBucketName",
            value=self.source_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "DestinationBucketName",
            value=self.destination_bucket.bucket_name,
        )
        CfnOutput(
            self,
            "TableName",
            value=self.table.table_name,
        )

#!/usr/bin/env python3

import aws_cdk as cdk

from backup_system.cleaner_stack import CleanerStack
from backup_system.replicator_stack import ReplicatorStack
from backup_system.storage_stack import StorageStack


app = cdk.App()

storage_stack = StorageStack(
    app,
    "BackupStorageStack",
)

replicator_stack = ReplicatorStack(
    app,
    "BackupReplicatorStack",
    event_queue=storage_stack.replicator_queue,
    source_bucket=storage_stack.source_bucket,
    destination_bucket=storage_stack.destination_bucket,
    table=storage_stack.table,
)

cleaner_stack = CleanerStack(
    app,
    "BackupCleanerStack",
    destination_bucket=storage_stack.destination_bucket,
    table=storage_stack.table,
)

app.synth()

# Object Backup System

This project implements the required object backup system in Python with AWS CDK.

## Architecture

- `Bucket Src`: source S3 bucket.
- `Bucket Dst`: destination S3 bucket that stores up to 3 most recent copies per source object.
- `Table T`: DynamoDB table that tracks the mapping between each source object and its copies.
- `ReplicatorQueue`: SQS queue that receives S3 events from `Bucket Src` and invokes `Replicator`.
- `Replicator` Lambda:
  - Triggered by S3 `PUT` and `DELETE` events from `Bucket Src` through SQS.
  - On `PUT`, creates a new copy in `Bucket Dst`, records the mapping in DynamoDB, and deletes the oldest active copy if there are now more than 3 active copies.
  - On `DELETE`, marks all active copies for that source object as `DISOWNED` with a `disowned_at` timestamp.
- `Cleaner` Lambda:
  - Triggered every minute by EventBridge.
  - Queries DynamoDB for `DISOWNED` copies older than 10 seconds, deletes them from `Bucket Dst`, and removes their DynamoDB records.

## DynamoDB Design (No Scan)

### Table

- Table name: created by CDK
- Partition key: `original_key` (String)
- Sort key: `copy_key` (String)

Each copy is stored as one item:

- `original_key = source object key`
- `copy_key = replicated object key in Bucket Dst`

Other attributes:

- `created_at`
- `status` (`ACTIVE` or `DISOWNED`)
- `disowned_at` (Number, only present when disowned)

### GSI

- Index name: `StatusIndex`
- Partition key: `status`
- Sort key: `disowned_at`

### Access Patterns

1. `Replicator` on `PUT`
   - `Query` table by `original_key = source_key` to get the current copies for that source object.
   - Filter in code for items where `status = ACTIVE`.
   - If active copies exceed 3, delete the oldest active copy by using the oldest item's `copy_key` and DynamoDB keys.

2. `Replicator` on `DELETE`
   - `Query` table by `original_key = source_key` with `FilterExpression status = ACTIVE`.
   - Update matching items to set `status = DISOWNED` and `disowned_at`.

3. `Cleaner`
   - `Query` GSI `StatusIndex` where:
     - `status = DISOWNED`
     - `disowned_at <= now - 10 seconds`
   - Delete the S3 copy and then delete the DynamoDB item.

No DynamoDB scan is used anywhere in this design.

### Destination Bucket Naming

- The replicated S3 object key is the same value stored in DynamoDB as `copy_key`.
- Format: `{original_key}#{created_at_ms}`
- Example: `Assignment1.txt#1711500000123`

## Project Layout

- `app.py`: CDK entrypoint
- `backup_system/storage_stack.py`: S3 buckets and DynamoDB table
- `backup_system/replicator_stack.py`: Replicator Lambda and SQS event source
- `backup_system/cleaner_stack.py`: Cleaner Lambda and EventBridge schedule
- `lambdas/replicator/handler.py`: Replicator handler
- `lambdas/cleaner/handler.py`: Cleaner handler

## Deployment

1. Create and activate a Python virtual environment.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Bootstrap CDK if needed:

```bash
cdk bootstrap
```

4. Deploy:

```bash
cdk deploy --all
```

## Destroy

Before the demo, destroy the stacks:

```bash
cdk destroy --all
```

## Demo Notes

- All infrastructure in this submission is created by CDK.
- No AWS resource needs to be manually created in the console.

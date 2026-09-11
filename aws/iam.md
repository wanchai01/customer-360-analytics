# IAM — Access Control (Section 19)

## Roles in this architecture

| Role | Assumed by | Purpose |
|------|------------|---------|
| `Customer360-EMR-ExecutionRole` | EMR cluster instances (EC2 instance profile) | Spark jobs reading/writing S3 `raw/`/`processed/`/`curated/` |
| `Customer360-MWAA-ExecutionRole` | Amazon MWAA (Airflow) | Triggering EMR steps, reading DAG code from S3, writing Airflow logs to CloudWatch, reading DB credentials from Secrets Manager |
| `powerbi_reader` (DB user, not an IAM role) | Power BI's Postgres connection | `SELECT`-only on `warehouse.*` and `analytics.*` schemas — a database-level role, since RDS PostgreSQL access control is via DB users/roles, not IAM policies, unless using IAM database authentication (optional, noted below) |

## Least-privilege policy: EMR execution role

Scoped to exactly this project's bucket, not `s3:*` on every bucket
in the account — the difference between "this pipeline can read/write
its own data lake" and "this pipeline's compromised credentials can
read every bucket in the AWS account":

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DataLakeReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::customer-360-data/*"
    },
    {
      "Sid": "DataLakeListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::customer-360-data"
    },
    {
      "Sid": "ReadDbCredentials",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:*:*:secret:prod/customer360/db-password-*"
    }
  ]
}
```

Notably **absent**: `s3:DeleteBucket`, `s3:PutBucketPolicy`,
`s3:*Public*` (any public-access-related action), and anything scoped
to `Resource: "*"`. A compromised EMR instance role under this policy
can corrupt or delete objects *within* this one bucket — a real risk,
which is why S3 versioning is recommended alongside this policy as a
recovery path — but it cannot delete the bucket itself, change its
public-access settings, or touch any other resource in the account.

## Least-privilege policy: MWAA execution role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadDagCode",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::customer-360-mwaa-dags",
        "arn:aws:s3:::customer-360-mwaa-dags/*"
      ]
    },
    {
      "Sid": "SubmitEmrSteps",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:DescribeCluster"
      ],
      "Resource": "arn:aws:elasticmapreduce:*:*:cluster/*",
      "Condition": {
        "StringEquals": { "aws:ResourceTag/Project": "customer-360" }
      }
    },
    {
      "Sid": "WriteOwnLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:airflow-customer360-*"
    },
    {
      "Sid": "ReadDbCredentials",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:*:*:secret:prod/customer360/*"
    }
  ]
}
```

Note the `aws:ResourceTag/Project` condition on the EMR permissions —
scoping by tag rather than by ARN wildcard means this role can only
operate on EMR clusters explicitly tagged for this project, not any
cluster anyone in the account happens to spin up.

## Database-level access control (RDS)

IAM policies control *AWS API* access (creating snapshots, describing
the instance, etc.) — they do not by themselves control *SQL* access
to the data inside RDS. That's handled the same way Phase 3 already
set it up locally:

- `customer360_app` — owns and can read/write `staging`, `warehouse`,
  and `analytics` schemas (the pipeline's own user).
- `airflow_app` — owns only the Airflow metadata database, with no
  grants on `customer360`'s own schemas at all (Phase 3's original
  least-privilege design, unchanged).
- A third, **new-for-production** role worth adding: `powerbi_reader`,
  granted `SELECT` only on `warehouse.*` and `analytics.*` (not
  `staging.*`, which can contain not-yet-quality-checked dirty data
  that has no business appearing on an executive dashboard):

```sql
CREATE ROLE powerbi_reader WITH LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA warehouse, analytics TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, analytics TO powerbi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse, analytics
    GRANT SELECT ON TABLES TO powerbi_reader;
```

(**IAM database authentication** is an alternative to a static
password for this role — RDS can accept short-lived auth tokens
signed by IAM instead, removing one more long-lived credential from
the system. Noted here as the natural next hardening step, not
implemented in this project's DDL since it requires an actual AWS
account to configure and test.)

# Adding a New AWS Hybrid Worker

This guide walks through deploying an additional Prefect hybrid worker on AWS EC2
that connects to the SPCS-hosted Prefect server.

## Architecture

```
┌──────────────────────────┐     HTTPS     ┌─────────────────────────┐
│  EC2 Instance            │  ──────────►  │  SPCS (Snowflake)       │
│  ┌────────────────────┐  │               │  ┌───────────────────┐  │
│  │  nginx auth-proxy  │──┼───────────────┼─►│  PF_SERVER         │  │
│  │  (injects PAT)     │  │               │  └───────────────────┘  │
│  └────────┬───────────┘  │               └─────────────────────────┘
│           │ :4200        │
│  ┌────────▼───────────┐  │
│  │  prefect worker    │  │
│  │  --pool <pool>     │  │
│  └────────────────────┘  │
└──────────────────────────┘
```

The nginx auth-proxy sidecar injects a Snowflake PAT into every request,
allowing the Prefect worker to connect to SPCS without modification.

Flow code is delivered via `git_clone` — the worker pulls from git before each run.

## Prerequisites

1. **AWS CLI v2** authenticated with a profile that has EC2 + SSM permissions
2. **Snowflake PAT** for the `PREFECT_SVC` service user
3. **SPCS endpoint** hostname (from `SHOW ENDPOINTS IN SERVICE PF_SERVER`)
4. **Git access token** for the private repo (GitLab project token or GitHub PAT)
5. **Prefect work pool** created on the server

## Step-by-Step

### 1. Create the Prefect work pool

Create a new work pool on the Prefect server. This only needs to happen once per pool.

```bash
# Set your Prefect API URL (via auth proxy or direct)
export PREFECT_API_URL="http://localhost:4201/api"

# Create the pool
prefect work-pool create <pool-name> --type process
```

Example: `prefect work-pool create aws-pool-backup --type process`

### 2. Add pool to pools.yaml

Add an entry to `pools.yaml` in the project root:

```yaml
pools:
  # ... existing pools ...

  aws-backup:                          # unique key
    type: external
    pool_name: aws-pool-backup         # must match step 1
    suffix: aws-backup                 # deployment name suffix
    tag: aws-backup                    # tag for filtering in UI
    worker_dir: workers/aws            # reuse existing AWS worker dir
```

### 3. Add deployments to prefect.yaml

For each flow, add a deployment targeting the new pool. In `flows/prefect.yaml`,
add entries like:

```yaml
- name: example-flow-aws-backup
  entrypoint: example_flow.py:example_flow
  work_pool:
    name: aws-pool-backup
  tags: [aws-backup, example]
  parameters:
    name: "Prefect-SPCS"
  schedules:
    - interval: 3600
  enforce_parameter_schema: true
  description: "Hello world flow on aws-pool-backup"
```

Repeat for all 6 flows: `example_flow`, `snowflake_etl`, `external_api_flow`,
`e2e_test_flow`, `analytics_revenue`, `quarterly_report`.

### 4. Deploy flow definitions

```bash
cd flows/
uv run --project /path/to/prefect-spcs prefect deploy --all \
  --prefect-file /path/to/prefect-spcs/prefect.yaml
```

### 5. Set environment variables

```bash
export SNOWFLAKE_PAT="<your-snowflake-pat>"
export SPCS_ENDPOINT="<xxxxx-orgname-acctname.snowflakecomputing.app>"
export GIT_ACCESS_TOKEN="<your-git-token>"
export SNOWFLAKE_ACCOUNT="<ORGNAME-ACCTNAME>"
export SNOWFLAKE_USER="PREFECT_SVC"

# Optional overrides for the setup script:
export AWS_INSTANCE_NAME="<your-prefix>-prefect-worker-<pool-name>"
# Override the pool name in docker-compose if different from aws-pool:
# (see step 6)
```

### 6. Customize docker-compose for the new pool

If the new worker targets a different pool name (e.g., `aws-pool-backup` instead
of `aws-pool`), you need to update the worker command. The easiest approach is to
override via the `.env` on the EC2 instance or modify the user-data in the setup
script.

**Option A — Modify setup script (recommended for automation):**

Edit the embedded `docker-compose.aws.yaml` in `setup_aws_worker.sh` and change
the worker command line:

```yaml
command: prefect worker start --pool aws-pool-backup --type process
```

**Option B — Override after bootstrap via SSM:**

After the instance is running, use SSM to edit the compose file:

```bash
aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["cd /opt/prefect-aws && sed -i \"s/--pool aws-pool/--pool aws-pool-backup/\" docker-compose.aws.yaml && docker compose -f docker-compose.aws.yaml up -d"]' \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

### 7. Launch the EC2 instance

```bash
./workers/aws/setup_aws_worker.sh
```

The script will:
1. Create an EC2 key pair (if not exists)
2. Find the latest full Amazon Linux 2023 AMI (not minimal!)
3. Build user-data that installs Docker + Compose + Buildx
4. Launch a `t3.small` instance on the private subnet
5. Inject secrets via user-data placeholders

Bootstrap takes ~4-5 minutes. The worker will appear in the Prefect UI
under the target work pool.

### 8. Verify the worker

Check bootstrap logs via SSM (no SSH needed on private subnet):

```bash
# Check bootstrap log
aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["cat /var/log/prefect-setup.log"]' \
  --region us-west-2 \
  --profile <AWS_PROFILE>

# Check docker container status
aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["cd /opt/prefect-aws && docker compose -f docker-compose.aws.yaml ps"]' \
  --region us-west-2 \
  --profile <AWS_PROFILE>

# Check worker logs
aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["cd /opt/prefect-aws && docker compose -f docker-compose.aws.yaml logs --tail 30"]' \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

To read SSM command output:

```bash
aws ssm get-command-invocation \
  --command-id <command-id> \
  --instance-id <instance-id> \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

### 9. Test with a flow run

Trigger a test run on the new pool:

```bash
export PREFECT_API_URL="http://localhost:4201/api"
prefect deployment run 'example-flow/example-flow-aws-backup'
```

Check in the Prefect UI that the run completes successfully.

### 10. Tear down (when done)

Instances launch with **termination protection enabled**. You must disable it first:

```bash
aws ec2 modify-instance-attribute \
  --instance-id <instance-id> \
  --no-disable-api-termination \
  --region us-west-2 \
  --profile <AWS_PROFILE>

aws ec2 terminate-instances \
  --instance-ids <instance-id> \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

Optionally delete the work pool:

```bash
prefect work-pool delete aws-pool-backup
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| cloud-init fails | Use full AL2023 AMI (`al2023-ami-2023*`), NOT `al2023-ami-minimal-*` |
| `buildx not found` | Buildx must be installed separately on AL2023 (setup script handles this) |
| Worker not appearing | Check bootstrap log via SSM; ensure NAT gateway allows outbound HTTPS |
| Flows crash with empty `GIT_REPO_URL` | Ensure `GIT_REPO_URL` and `GIT_BRANCH` are in the `.env` on the instance |
| SSH times out | Instance is on private subnet with no public IP; use SSM (see below) |
| SCP blocks VPC creation | Use existing shared VPC (already configured in setup script defaults) |

## Accessing EC2 Instances

EC2 instances are on a **private subnet** (no public IP). SSH from the internet will time out.
Use AWS Systems Manager (SSM) instead — it tunnels through the SSM service, no inbound ports needed.

### Prerequisites (one-time)

Install the SSM Session Manager plugin:

```bash
# macOS ARM (Apple Silicon)
curl -o /tmp/ssm-plugin.pkg "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/mac_arm64/session-manager-plugin.pkg"
sudo installer -pkg /tmp/ssm-plugin.pkg -target /
sudo ln -sf /usr/local/sessionmanagerplugin/bin/session-manager-plugin /usr/local/bin/session-manager-plugin

# Verify
session-manager-plugin --version
```

### Option 1: SSM Interactive Shell (recommended)

No SSH key needed. Drops you into a root shell immediately:

```bash
aws ssm start-session \
  --target <instance-id> \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

Once in the shell, check worker status:

```bash
cd /opt/prefect-aws
docker ps
docker compose -f docker-compose.aws.yaml logs --tail 20
cat /var/log/prefect-setup.log
```

### Option 2: SSM Remote Command (non-interactive)

Run commands without opening a session. Good for scripting and quick checks:

```bash
# Send command
COMMAND_ID=$(aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name "AWS-RunShellScript" \
  --parameters '{"commands":["docker ps","cd /opt/prefect-aws && docker compose -f docker-compose.aws.yaml logs --tail 10"]}' \
  --region us-west-2 \
  --profile <AWS_PROFILE> \
  --query 'Command.CommandId' \
  --output text)

# Wait a few seconds, then get output
aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" \
  --instance-id <instance-id> \
  --region us-west-2 \
  --profile <AWS_PROFILE>
```

> **Note:** Avoid Go template syntax (`{{.Names}}`) in SSM commands — the braces
> cause shell parse errors. Use plain `docker ps` instead.

### Option 3: SSH over SSM Tunnel

Use when you need SCP file transfers or SSH-specific tooling (VS Code Remote, etc.):

```bash
# Terminal 1: Start the tunnel (keeps running in foreground)
aws ssm start-session \
  --target <instance-id> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["22"],"localPortNumber":["2222"]}' \
  --region us-west-2 \
  --profile <AWS_PROFILE>

# Terminal 2: SSH through the tunnel
chmod 600 workers/aws/<YOUR_KEY_NAME>.pem
ssh -p 2222 -i workers/aws/<YOUR_KEY_NAME>.pem ec2-user@localhost
```

### Running Instances

| Pool | Instance ID | Private IP | Name Tag |
|------|-------------|------------|----------|
| `aws-pool` | `<PRIMARY_INSTANCE_ID>` | `<PRIMARY_PRIVATE_IP>` | `<YOUR_PREFIX>-prefect-worker-aws` |
| `aws-pool-backup` | `<BACKUP_INSTANCE_ID>` | `<BACKUP_PRIVATE_IP>` | `<YOUR_PREFIX>-prefect-worker-aws-backup` |

> **No `Schedule` tag** — the AWS Instance Scheduler ignores untagged instances.
> Combined with `--disable-api-termination`, the instance stays running and
> cannot be accidentally terminated.

Each instance runs 7 Docker containers:
- `prefect-aws-prefect-worker-aws-1` — Prefect ProcessWorker polling its pool
- `prefect-aws-auth-proxy-1` — nginx reverse proxy to SPCS endpoint (healthy)
- `prefect-aws-prometheus-agent-1` — metrics remote-write to SPCS Prometheus
- `prefect-aws-promtail-1` — log shipping to SPCS Loki
- `prefect-aws-node-exporter-1` — host metrics
- `prefect-aws-cadvisor-1` — container metrics
- `prefect-aws-auth-proxy-monitor-1` — monitoring auth-proxy

> **CRITICAL: Region is `us-west-2`**. The Snowflake account is in `us-east-1` but the
> EC2 instances are in `us-west-2` (SE demo account default region). If you search for
> instances in `us-east-1` they will appear as `InvalidInstanceID.NotFound`. Always use
> `--region us-west-2` for all AWS CLI commands.

### Verifying Workers via SSM

These instances are on private subnets with no public IP. Use SSM (not SSH) to check status:

```bash
# Check Docker containers on primary worker
CMDID=$(aws ssm send-command \
  --instance-ids <PRIMARY_INSTANCE_ID> \
  --document-name "AWS-RunShellScript" \
  --parameters '{"commands":["docker ps"]}' \
  --region us-west-2 \
  --profile <AWS_PROFILE> \
  --query 'Command.CommandId' --output text)
sleep 5
aws ssm get-command-invocation \
  --command-id "$CMDID" \
  --instance-id <PRIMARY_INSTANCE_ID> \
  --region us-west-2 \
  --profile <AWS_PROFILE> \
  --query 'StandardOutputContent' --output text

# Check SSM agent status for both instances
aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=<PRIMARY_INSTANCE_ID>,<BACKUP_INSTANCE_ID>" \
  --query "InstanceInformationList[].[InstanceId,PingStatus,IPAddress]" \
  --region us-west-2 --profile <AWS_PROFILE> --output table
```

## AWS Environment Details

| Resource | Value |
|----------|-------|
| Account | `<AWS_ACCOUNT_ID>` |
| Profile | `<AWS_PROFILE>` |
| Region | **`us-west-2`** (not us-east-1 — see note above) |
| VPC | `<VPC_ID>` |
| Subnet | `<SUBNET_ID>` (private, us-west-2a) |
| Security Group | `<SECURITY_GROUP_ID>` |
| Instance Type | `t3.small` |
| AMI | Amazon Linux 2023 (full, not minimal) |
| VPC DNS Resolver | `169.254.169.253` |
| Access Method | SSM only (private subnet, no public IP) |

#!/usr/bin/env bash
# =============================================================================
# setup_aws_worker.sh — Provision an EC2 instance and start a Prefect worker
#                        that connects to the SPCS-hosted server.
#
# Architecture:
#   nginx auth-proxy (injects Snowflake PAT) → SPCS public endpoint
#   prefect worker → http://auth-proxy:4200/api
#   monitoring sidecars (node-exporter, cadvisor, prometheus-agent, promtail)
#     → remote_write to SPCS Prometheus via auth-proxy-monitor
#
# Prerequisites:
#   - AWS CLI v2 authenticated (profile with EC2 permissions)
#   - SNOWFLAKE_PAT set to a Snowflake programmatic access token
#   - SPCS_ENDPOINT set to the SPCS server hostname (no https://)
#   - GIT_ACCESS_TOKEN set for private repo access
#   - SPCS_MONITOR_ENDPOINT set to PF_MONITOR Prometheus endpoint hostname
#   - SPCS_MONITOR_LOKI_ENDPOINT set to PF_MONITOR Loki endpoint hostname
#
# Usage:
#   export SNOWFLAKE_PAT="<your-snowflake-pat>"
#   export SPCS_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   export GIT_ACCESS_TOKEN="<your-git-token>"
#   export SNOWFLAKE_ACCOUNT="ORGNAME-ACCTNAME"
#   export SNOWFLAKE_USER="PREFECT_SVC"
#   export SPCS_MONITOR_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   export SPCS_MONITOR_LOKI_ENDPOINT="xxxxx-orgname-acctname.snowflakecomputing.app"
#   ./workers/aws/setup_aws_worker.sh
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via env vars)
# ---------------------------------------------------------------------------
AWS_PROFILE="${AWS_PROFILE:?Set AWS_PROFILE}"
AWS_REGION="${AWS_REGION:-us-west-2}"
INSTANCE_TYPE="${AWS_INSTANCE_TYPE:-t3.small}"
KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME}"
INSTANCE_NAME="${AWS_INSTANCE_NAME:-prefect-worker-aws}"

# Networking — SE demo account shared VPC in us-west-2
SUBNET_ID="${AWS_SUBNET_ID:?Set AWS_SUBNET_ID}"
SECURITY_GROUP_ID="${AWS_SECURITY_GROUP_ID:?Set AWS_SECURITY_GROUP_ID}"

# Required secrets (passed to EC2 via user-data)
PAT="${SNOWFLAKE_PAT:?Set SNOWFLAKE_PAT to your Snowflake programmatic access token}"
ENDPOINT="${SPCS_ENDPOINT:?Set SPCS_ENDPOINT to your SPCS server hostname (no https://)}"
GIT_REPO="${GIT_REPO_URL:-https://github.com/your-org/your-repo.git}"
GIT_BR="${GIT_BRANCH:-main}"
GIT_TOKEN="${GIT_ACCESS_TOKEN:-}"
SF_ACCOUNT="${SNOWFLAKE_ACCOUNT:-}"
SF_USER="${SNOWFLAKE_USER:-}"
SF_PASSWORD="${SNOWFLAKE_PASSWORD:-}"
SF_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
SF_DATABASE="${SNOWFLAKE_DATABASE:-PREFECT_DB}"
SF_SCHEMA="${SNOWFLAKE_SCHEMA:-PREFECT_SCHEMA}"

# Monitoring endpoints (required — sidecars always start with the worker)
MONITOR_PROM="${SPCS_MONITOR_ENDPOINT:-}"
MONITOR_LOKI="${SPCS_MONITOR_LOKI_ENDPOINT:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export AWS_PROFILE AWS_REGION

echo "=== Creating EC2 instance for hybrid Prefect worker ==="
echo "Region:        $AWS_REGION"
echo "Instance Type: $INSTANCE_TYPE"
echo "Key Pair:      $KEY_NAME"
echo "Subnet:        $SUBNET_ID"
echo "Security Group:$SECURITY_GROUP_ID"
echo "SPCS Endpoint: $ENDPOINT"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Ensure key pair exists
# ---------------------------------------------------------------------------
if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$AWS_REGION" &>/dev/null; then
    echo "Creating key pair '$KEY_NAME'..."
    aws ec2 create-key-pair \
        --key-name "$KEY_NAME" \
        --key-type ed25519 \
        --region "$AWS_REGION" \
        --query 'KeyMaterial' \
        --output text > "${SCRIPT_DIR}/${KEY_NAME}.pem"
    chmod 600 "${SCRIPT_DIR}/${KEY_NAME}.pem"
    echo "  Private key saved to: ${SCRIPT_DIR}/${KEY_NAME}.pem"
    echo "  ⚠ Back this up — it cannot be downloaded again."
else
    echo "Key pair '$KEY_NAME' already exists."
fi

# ---------------------------------------------------------------------------
# Step 2: Find latest Amazon Linux 2023 AMI
# ---------------------------------------------------------------------------
echo "Looking up latest Amazon Linux 2023 AMI (full, not minimal)..."
AMI_ID=$(aws ec2 describe-images \
    --region "$AWS_REGION" \
    --owners amazon \
    --filters \
        "Name=name,Values=al2023-ami-2023*-x86_64" \
        "Name=state,Values=available" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
    --output text)

if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
    echo "ERROR: Could not find Amazon Linux 2023 AMI. Falling back to known AMI."
    AMI_ID="ami-0b20a6f09f8571ff1"
fi
echo "  AMI: $AMI_ID"

# ---------------------------------------------------------------------------
# Step 3: Build user-data script
#   Installs Docker + Docker Compose, writes compose files, starts services.
#   All secrets are injected via user-data (never stored in AMI or S3).
# ---------------------------------------------------------------------------
USER_DATA=$(cat <<'OUTEREOF'
#!/bin/bash
set -euo pipefail
exec > /var/log/prefect-setup.log 2>&1

echo "=== Prefect worker bootstrap starting ==="

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Install Docker Compose plugin
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Install Docker Buildx (required by Compose for --build)
BUILDX_URL=$(curl -s https://api.github.com/repos/docker/buildx/releases/latest \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print([a['browser_download_url'] for a in r['assets'] if 'linux-amd64' in a['name'] and not a['name'].endswith('.sbom') and not a['name'].endswith('.provenance')][0])")
curl -SL "$BUILDX_URL" -o /usr/local/lib/docker/cli-plugins/docker-buildx
chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx

# Wait for Docker to be ready
for i in $(seq 1 30); do docker info &>/dev/null && break || sleep 2; done

WORKDIR=/opt/prefect-aws
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# Write nginx.conf template
cat > nginx.conf <<'NGINXEOF'
worker_processes 1;

events {
    worker_connections 256;
}

http {
    resolver 169.254.169.253 valid=60s;

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      '';
    }

    server {
        listen 4200;

        location / {
            proxy_pass https://${SPCS_ENDPOINT};
            proxy_ssl_server_name on;
            proxy_http_version 1.1;

            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;

            proxy_set_header Authorization "Snowflake Token=\"${SNOWFLAKE_PAT}\"";
            proxy_set_header Host ${SPCS_ENDPOINT};

            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            proxy_connect_timeout 30s;
            proxy_read_timeout 120s;
            proxy_send_timeout 30s;
        }
    }
}
NGINXEOF

# Write Dockerfile
cat > Dockerfile.worker <<'DOCKEREOF'
FROM prefecthq/prefect:3-python3.12
RUN pip install --no-cache-dir snowflake-connector-python
DOCKEREOF

# Write docker-compose
cat > docker-compose.aws.yaml <<'COMPOSEEOF'
services:
  auth-proxy:
    image: nginx:alpine
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf.template:ro
    environment:
      SNOWFLAKE_PAT: "${SNOWFLAKE_PAT}"
      SPCS_ENDPOINT: "${SPCS_ENDPOINT}"
    command:
      - /bin/sh
      - -c
      - |
        envsubst '$$SNOWFLAKE_PAT $$SPCS_ENDPOINT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && nginx -g 'daemon off;'
    ports:
      - "4201:4200"
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:4200/api/health"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 5s
    restart: on-failure

  prefect-worker-aws:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      PREFECT_API_URL: "http://auth-proxy:4200/api"
      GIT_REPO_URL: "${GIT_REPO_URL:-}"
      GIT_BRANCH: "${GIT_BRANCH:-main}"
      GIT_ACCESS_TOKEN: "${GIT_ACCESS_TOKEN:-}"
      SNOWFLAKE_ACCOUNT: "${SNOWFLAKE_ACCOUNT:-}"
      SNOWFLAKE_PAT: "${SNOWFLAKE_PAT:-}"
      SNOWFLAKE_USER: "${SNOWFLAKE_USER:-}"
      SNOWFLAKE_PASSWORD: "${SNOWFLAKE_PASSWORD:-}"
      SNOWFLAKE_WAREHOUSE: "${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
      SNOWFLAKE_DATABASE: "${SNOWFLAKE_DATABASE:-PREFECT_DB}"
      SNOWFLAKE_SCHEMA: "${SNOWFLAKE_SCHEMA:-PREFECT_SCHEMA}"
    command: prefect worker start --pool aws-pool --type process --with-healthcheck
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,urllib.error,sys\ntry:\n urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=5)\nexcept urllib.error.HTTPError:\n sys.exit(0)\nexcept Exception:\n sys.exit(1)"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    depends_on:
      auth-proxy:
        condition: service_healthy
    restart: unless-stopped
COMPOSEEOF

# Write .env file with secrets (only accessible on this instance)
cat > .env <<ENVEOF
SNOWFLAKE_PAT=__SNOWFLAKE_PAT__
SPCS_ENDPOINT=__SPCS_ENDPOINT__
GIT_REPO_URL=__GIT_REPO_URL__
GIT_BRANCH=__GIT_BRANCH__
GIT_ACCESS_TOKEN=__GIT_ACCESS_TOKEN__
SNOWFLAKE_ACCOUNT=__SNOWFLAKE_ACCOUNT__
SNOWFLAKE_USER=__SNOWFLAKE_USER__
SNOWFLAKE_PASSWORD=__SNOWFLAKE_PASSWORD__
SNOWFLAKE_WAREHOUSE=__SNOWFLAKE_WAREHOUSE__
SNOWFLAKE_DATABASE=__SNOWFLAKE_DATABASE__
SNOWFLAKE_SCHEMA=__SNOWFLAKE_SCHEMA__
SPCS_MONITOR_ENDPOINT=__SPCS_MONITOR_ENDPOINT__
SPCS_MONITOR_LOKI_ENDPOINT=__SPCS_MONITOR_LOKI_ENDPOINT__
WORKER_LOCATION=aws-__AWS_REGION__
WORKER_POOL=aws-pool
DNS_RESOLVER=169.254.169.253
ENVEOF
chmod 600 .env

# Always write monitoring configs — .env has all needed vars.
# docker-compose.monitoring.yml validates via ${SPCS_MONITOR_ENDPOINT:?...}.
mkdir -p vm-agents

    # nginx auth-proxy for monitoring traffic
    cat > vm-agents/nginx-monitor.conf <<'NGINXMONEOF'
worker_processes 1;
error_log /dev/stderr warn;

events {
    worker_connections 128;
}

http {
    resolver ${DNS_RESOLVER} valid=30s ipv6=off;

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    server {
        listen 4210;

        location / {
            proxy_pass https://${SPCS_MONITOR_ENDPOINT}:443;
            proxy_http_version 1.1;
            proxy_ssl_server_name on;
            proxy_ssl_name ${SPCS_MONITOR_ENDPOINT};
            proxy_set_header Host ${SPCS_MONITOR_ENDPOINT};
            proxy_set_header Authorization "Snowflake Token=\"${SNOWFLAKE_PAT}\"";
            proxy_set_header Connection "";
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 10s;
            proxy_send_timeout 30s;
            proxy_read_timeout 30s;
            client_max_body_size 10m;
        }
    }

    server {
        listen 4220;

        location / {
            proxy_pass https://${SPCS_MONITOR_LOKI_ENDPOINT}:443;
            proxy_http_version 1.1;
            proxy_ssl_server_name on;
            proxy_ssl_name ${SPCS_MONITOR_LOKI_ENDPOINT};
            proxy_set_header Host ${SPCS_MONITOR_LOKI_ENDPOINT};
            proxy_set_header Authorization "Snowflake Token=\"${SNOWFLAKE_PAT}\"";
            proxy_set_header Connection "";
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 10s;
            proxy_send_timeout 30s;
            proxy_read_timeout 30s;
            client_max_body_size 10m;
        }
    }
}
NGINXMONEOF

    # Prometheus agent config
    cat > vm-agents/prometheus-agent.yml <<'PROMAGENTEOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    worker_location: "${WORKER_LOCATION}"
    worker_pool: "${WORKER_POOL}"

scrape_configs:
  - job_name: node-exporter
    static_configs:
      - targets:
          - node-exporter:9100

  - job_name: cadvisor
    static_configs:
      - targets:
          - cadvisor:8080

  - job_name: prefect-worker
    metrics_path: /api/health
    fallback_scrape_protocol: PrometheusText0.0.4
    static_configs:
      - targets:
          - auth-proxy:4200

remote_write:
  - url: "http://auth-proxy-monitor:4210/api/v1/write"
    protobuf_message: "io.prometheus.write.v2.Request"
    queue_config:
      capacity: 5000
      max_shards: 5
      max_samples_per_send: 1000
      batch_send_deadline: 5s
PROMAGENTEOF

    # Promtail config
    cat > vm-agents/promtail-config.yaml <<'PROMTAILEOF'
server:
  http_listen_port: 9080
  grpc_listen_port: 0
  log_level: warn

positions:
  filename: /tmp/positions.yaml

clients:
  - url: "http://auth-proxy-monitor:4220/loki/api/v1/push"
    external_labels:
      worker_location: "${WORKER_LOCATION}"
      worker_pool: "${WORKER_POOL}"

scrape_configs:
  - job_name: docker
    static_configs:
      - targets:
          - localhost
        labels:
          job: docker
          __path__: /var/log/containers/*.log
    pipeline_stages:
      - docker: {}
      - labels:
          container_name:
      - timestamp:
          source: time
          format: RFC3339Nano
PROMTAILEOF

    # Monitoring docker-compose overlay
    cat > docker-compose.monitoring.yml <<'MONCOMPOSEEOF'
services:
  node-exporter:
    image: prom/node-exporter:v1.9.1
    pid: host
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - --path.procfs=/host/proc
      - --path.sysfs=/host/sys
      - --path.rootfs=/rootfs
      - --collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)
    restart: unless-stopped

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.52.1
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    devices:
      - /dev/kmsg
    restart: unless-stopped

  auth-proxy-monitor:
    image: nginx:alpine
    volumes:
      - ./vm-agents/nginx-monitor.conf:/etc/nginx/nginx.conf.template:ro
    environment:
      SNOWFLAKE_PAT: "${SNOWFLAKE_PAT}"
      SPCS_MONITOR_ENDPOINT: "${SPCS_MONITOR_ENDPOINT}"
      SPCS_MONITOR_LOKI_ENDPOINT: "${SPCS_MONITOR_LOKI_ENDPOINT}"
      DNS_RESOLVER: "${DNS_RESOLVER:-169.254.169.253}"
    command:
      - /bin/sh
      - -c
      - |
        envsubst '$$SNOWFLAKE_PAT $$SPCS_MONITOR_ENDPOINT $$SPCS_MONITOR_LOKI_ENDPOINT $$DNS_RESOLVER' \
          < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf \
          && nginx -g 'daemon off;'
    restart: unless-stopped

  prometheus-agent:
    image: prom/prometheus:v3.4.1
    command:
      - --config.file=/etc/prometheus/prometheus-agent.yml
      - --agent
    volumes:
      - ./vm-agents/prometheus-agent.yml:/etc/prometheus/prometheus-agent.yml:ro
    environment:
      WORKER_LOCATION: "${WORKER_LOCATION}"
      WORKER_POOL: "${WORKER_POOL}"
    depends_on:
      - node-exporter
      - cadvisor
      - auth-proxy-monitor
    restart: unless-stopped

  promtail:
    image: grafana/promtail:3.5.0
    command:
      - -config.file=/etc/promtail/promtail-config.yaml
      - -config.expand-env=true
    volumes:
      - ./vm-agents/promtail-config.yaml:/etc/promtail/promtail-config.yaml:ro
      - /var/lib/docker/containers:/var/log/containers:ro
      - /tmp/promtail-positions:/tmp
    environment:
      WORKER_LOCATION: "${WORKER_LOCATION}"
      WORKER_POOL: "${WORKER_POOL}"
    depends_on:
      - auth-proxy-monitor
    restart: unless-stopped
MONCOMPOSEEOF

# Always start with monitoring overlay — .env has all needed vars
echo "Starting worker + monitoring sidecars..."
docker compose -f docker-compose.aws.yaml -f docker-compose.monitoring.yml up -d --build

echo "=== Prefect worker bootstrap complete ==="
OUTEREOF
)

# Inject actual secret values into user-data
USER_DATA="${USER_DATA//__SNOWFLAKE_PAT__/$PAT}"
USER_DATA="${USER_DATA//__SPCS_ENDPOINT__/$ENDPOINT}"
USER_DATA="${USER_DATA//__GIT_REPO_URL__/$GIT_REPO}"
USER_DATA="${USER_DATA//__GIT_BRANCH__/$GIT_BR}"
USER_DATA="${USER_DATA//__GIT_ACCESS_TOKEN__/$GIT_TOKEN}"
USER_DATA="${USER_DATA//__SNOWFLAKE_ACCOUNT__/$SF_ACCOUNT}"
USER_DATA="${USER_DATA//__SNOWFLAKE_USER__/$SF_USER}"
USER_DATA="${USER_DATA//__SNOWFLAKE_PASSWORD__/$SF_PASSWORD}"
USER_DATA="${USER_DATA//__SNOWFLAKE_WAREHOUSE__/$SF_WAREHOUSE}"
USER_DATA="${USER_DATA//__SNOWFLAKE_DATABASE__/$SF_DATABASE}"
USER_DATA="${USER_DATA//__SNOWFLAKE_SCHEMA__/$SF_SCHEMA}"
USER_DATA="${USER_DATA//__SPCS_MONITOR_ENDPOINT__/$MONITOR_PROM}"
USER_DATA="${USER_DATA//__SPCS_MONITOR_LOKI_ENDPOINT__/$MONITOR_LOKI}"
USER_DATA="${USER_DATA//__AWS_REGION__/$AWS_REGION}"

# ---------------------------------------------------------------------------
# Step 4: Launch instance
# ---------------------------------------------------------------------------
echo "Launching EC2 instance..."
INSTANCE_ID=$(aws ec2 run-instances \
    --region "$AWS_REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --subnet-id "$SUBNET_ID" \
    --security-group-ids "$SECURITY_GROUP_ID" \
    --user-data "$USER_DATA" \
    --disable-api-termination \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "  Instance ID: $INSTANCE_ID"

# ---------------------------------------------------------------------------
# Step 5: Wait for running state
# ---------------------------------------------------------------------------
echo "Waiting for instance to reach 'running' state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

PRIVATE_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --region "$AWS_REGION" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' \
    --output text)

echo ""
echo "=== EC2 instance launched ==="
echo "Instance ID:  $INSTANCE_ID"
echo "Private IP:   $PRIVATE_IP"
echo "Name:         $INSTANCE_NAME"
echo ""
echo "The worker will appear in the Prefect UI under work pool 'aws-pool'"
echo "after user-data bootstrap completes (~4-5 minutes)."
echo ""
echo "To check bootstrap logs (via SSM — no SSH required):"
echo "  aws ssm send-command --instance-ids $INSTANCE_ID --document-name AWS-RunShellScript --parameters 'commands=[\"cat /var/log/prefect-setup.log\"]' --region $AWS_REGION --profile $AWS_PROFILE"
echo ""
echo "To check worker status (via SSM):"
echo "  aws ssm send-command --instance-ids $INSTANCE_ID --document-name AWS-RunShellScript --parameters 'commands=[\"cd /opt/prefect-aws && docker compose -f docker-compose.aws.yaml logs --tail 30\"]' --region $AWS_REGION --profile $AWS_PROFILE"
echo ""
echo "To terminate (requires disabling termination protection first):"
echo "  aws ec2 modify-instance-attribute --instance-id $INSTANCE_ID --no-disable-api-termination --region $AWS_REGION --profile $AWS_PROFILE"
echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $AWS_REGION --profile $AWS_PROFILE"

"""Tests for workers/*/nginx.conf — auth proxy templates.

Validates template variables, WebSocket support, auth header injection,
and proxy configuration for hybrid worker auth proxies (GCP and AWS).
"""

import pytest
from conftest import WORKERS_DIR

GCP_NGINX_CONF = WORKERS_DIR / "gcp" / "nginx.conf"
AWS_NGINX_CONF = WORKERS_DIR / "aws" / "nginx.conf"


@pytest.fixture(scope="module")
def nginx_content():
    assert GCP_NGINX_CONF.exists(), "GCP nginx.conf must exist"
    return GCP_NGINX_CONF.read_text()


@pytest.fixture(scope="module")
def aws_nginx_content():
    assert AWS_NGINX_CONF.exists(), "AWS nginx.conf must exist"
    return AWS_NGINX_CONF.read_text()


class TestNginxConfExists:
    def test_file_exists(self):
        assert GCP_NGINX_CONF.exists()

    def test_aws_file_exists(self):
        assert AWS_NGINX_CONF.exists()


class TestNginxTemplateVariables:
    def test_has_snowflake_pat_template_var(self, nginx_content):
        assert "${SNOWFLAKE_PAT}" in nginx_content, (
            "nginx.conf must use ${SNOWFLAKE_PAT} template variable"
        )

    def test_has_spcs_endpoint_template_var(self, nginx_content):
        assert "${SPCS_ENDPOINT}" in nginx_content, (
            "nginx.conf must use ${SPCS_ENDPOINT} template variable"
        )


class TestNginxProxyConfig:
    def test_has_websocket_support(self, nginx_content):
        assert "Upgrade" in nginx_content, "nginx.conf must support WebSocket Upgrade header"
        assert "Connection" in nginx_content, "nginx.conf must set Connection header for WebSocket"

    def test_injects_auth_header(self, nginx_content):
        assert "Authorization" in nginx_content, "nginx.conf must inject Authorization header"
        assert "Snowflake Token=" in nginx_content, (
            "Authorization header must use Snowflake Token format"
        )

    def test_listens_on_4200(self, nginx_content):
        assert "listen 4200" in nginx_content, (
            "nginx.conf must listen on port 4200 to match Prefect API"
        )

    def test_has_resolver(self, nginx_content):
        assert "resolver" in nginx_content, (
            "nginx.conf must have a resolver directive for DNS resolution"
        )

    def test_proxy_pass_uses_https(self, nginx_content):
        assert "proxy_pass https://" in nginx_content, (
            "proxy_pass must use HTTPS to reach SPCS endpoint"
        )

    def test_has_proxy_ssl_server_name(self, nginx_content):
        assert "proxy_ssl_server_name on" in nginx_content, "Must enable SNI for SPCS endpoint SSL"


# ---- AWS nginx.conf tests ----


class TestAwsNginxTemplateVariables:
    """AWS nginx.conf must use the same template variables as GCP."""

    def test_has_snowflake_pat_template_var(self, aws_nginx_content):
        assert "${SNOWFLAKE_PAT}" in aws_nginx_content, (
            "AWS nginx.conf must use ${SNOWFLAKE_PAT} template variable"
        )

    def test_has_spcs_endpoint_template_var(self, aws_nginx_content):
        assert "${SPCS_ENDPOINT}" in aws_nginx_content, (
            "AWS nginx.conf must use ${SPCS_ENDPOINT} template variable"
        )


class TestAwsNginxProxyConfig:
    """AWS nginx.conf must have the same proxy features as GCP."""

    def test_has_websocket_support(self, aws_nginx_content):
        assert "Upgrade" in aws_nginx_content, (
            "AWS nginx.conf must support WebSocket Upgrade header"
        )
        assert "Connection" in aws_nginx_content, (
            "AWS nginx.conf must set Connection header for WebSocket"
        )

    def test_injects_auth_header(self, aws_nginx_content):
        assert "Authorization" in aws_nginx_content, (
            "AWS nginx.conf must inject Authorization header"
        )
        assert "Snowflake Token=" in aws_nginx_content, (
            "Authorization header must use Snowflake Token format"
        )

    def test_listens_on_4200(self, aws_nginx_content):
        assert "listen 4200" in aws_nginx_content, (
            "AWS nginx.conf must listen on port 4200 to match Prefect API"
        )

    def test_proxy_pass_uses_https(self, aws_nginx_content):
        assert "proxy_pass https://" in aws_nginx_content, (
            "proxy_pass must use HTTPS to reach SPCS endpoint"
        )

    def test_has_proxy_ssl_server_name(self, aws_nginx_content):
        assert "proxy_ssl_server_name on" in aws_nginx_content, (
            "Must enable SNI for SPCS endpoint SSL"
        )


class TestAwsNginxDnsResolver:
    """AWS nginx.conf must use Amazon VPC DNS resolver, not Google DNS."""

    def test_uses_amazon_vpc_dns_resolver(self, aws_nginx_content):
        assert "169.254.169.253" in aws_nginx_content, (
            "AWS nginx.conf must use Amazon VPC DNS resolver 169.254.169.253"
        )

    def test_does_not_use_google_dns(self, aws_nginx_content):
        assert "8.8.8.8" not in aws_nginx_content, (
            "AWS nginx.conf must NOT use Google DNS 8.8.8.8 — use Amazon VPC DNS"
        )


class TestGcpNginxDnsResolver:
    """GCP nginx.conf must use Google DNS, not Amazon DNS."""

    def test_uses_google_dns(self, nginx_content):
        assert "8.8.8.8" in nginx_content, "GCP nginx.conf must use Google DNS resolver 8.8.8.8"

    def test_does_not_use_amazon_dns(self, nginx_content):
        assert "169.254.169.253" not in nginx_content, (
            "GCP nginx.conf must NOT use Amazon VPC DNS 169.254.169.253"
        )

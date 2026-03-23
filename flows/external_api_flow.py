"""Flow that calls an external REST API.

Demonstrates External Access Integration (EAI) on SPCS — the worker
must have outbound network access to reach httpbin.org.
"""

from hooks import on_flow_failure
from prefect import flow, task

# Allowed target URLs — prevents SSRF via deployment parameters.
ALLOWED_URLS = frozenset(
    {
        "https://httpbin.org/get",
        "https://httpbin.org/ip",
        "https://httpbin.org/headers",
    }
)


@task
def call_external_api(url: str) -> dict:
    """Make an HTTP GET request to an external API."""
    import requests

    if url not in ALLOWED_URLS:
        raise ValueError(f"URL not in allowlist: {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"External API response status: {response.status_code}")
    print(f"Origin IP: {data.get('origin', 'unknown')}")
    return data


@flow(
    name="external-api-flow",
    log_prints=True,
    on_failure=[on_flow_failure],
)
def external_api_flow(url: str = "https://httpbin.org/get"):
    """Call an external API to prove EAI outbound access works."""
    data = call_external_api(url)
    print(f"Response keys: {list(data.keys())}")
    return data


if __name__ == "__main__":
    external_api_flow()

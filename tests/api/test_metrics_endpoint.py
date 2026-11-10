"""
/metrics serves the node metrics and, after them, the HTTP API metrics of the
analytics middleware that the app factory installs.
"""

from typing import Optional

import pytest

VERSION_LABELS = ('route="/api/v0/version"', 'method="GET"', 'status="200"')


def version_requests_total(text: str) -> Optional[float]:
    """
    The value of the /api/v0/version request counter sample, matched whatever
    the label order (the multiprocess collector sorts labels by name).
    """
    for line in text.splitlines():
        if not line.startswith("aleph_http_requests_total{"):
            continue
        labels, _, value = line.rpartition(" ")
        if all(label in labels for label in VERSION_LABELS):
            return float(value)
    return None


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_http_metrics_next_to_node_metrics(
    ccn_api_client,
):
    response = await ccn_api_client.get("/api/v0/version")
    assert response.status == 200, await response.text()

    response = await ccn_api_client.get("/metrics")
    assert response.status == 200, await response.text()
    assert response.content_type == "text/plain"
    assert response.charset == "utf-8"
    text = await response.text()

    assert "pyaleph_status_sync_messages_total" in text
    value = version_requests_total(text)
    assert value is not None, text
    # Other tests in the same process may have hit the route too.
    assert value >= 1.0


@pytest.mark.asyncio
async def test_metrics_json_has_no_http_metrics(ccn_api_client):
    response = await ccn_api_client.get("/metrics.json")
    assert response.status == 200, await response.text()
    assert response.content_type == "application/json"
    assert "aleph_http_requests_total" not in await response.text()

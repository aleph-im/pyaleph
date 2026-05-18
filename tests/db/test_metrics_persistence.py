from aleph.db.accessors.metrics import _build_ccn_rows, _build_crn_rows


def test_build_crn_rows_full_payload():
    item_hash = "msg-1"
    crn_array = [
        {
            "measured_at": 1700000000.0,
            "node_id": "node-A",
            "base_latency": 0.1,
            "base_latency_ipv4": 0.11,
            "full_check_latency": 0.2,
            "diagnostic_vm_latency": 0.3,
        },
        {
            "measured_at": 1700000001.0,
            "node_id": "node-B",
            "base_latency": 0.4,
            "base_latency_ipv4": 0.41,
            "full_check_latency": 0.5,
            "diagnostic_vm_latency": 0.6,
        },
    ]
    rows = _build_crn_rows(item_hash, crn_array)
    assert len(rows) == 2
    assert rows[0] == {
        "item_hash": "msg-1",
        "node_id": "node-A",
        "measured_at": 1700000000.0,
        "base_latency": 0.1,
        "base_latency_ipv4": 0.11,
        "full_check_latency": 0.2,
        "diagnostic_vm_latency": 0.3,
    }


def test_build_crn_rows_missing_optional_fields():
    rows = _build_crn_rows("msg-2", [
        {"measured_at": 1700000000.0, "node_id": "node-A"},
    ])
    assert rows == [{
        "item_hash": "msg-2",
        "node_id": "node-A",
        "measured_at": 1700000000.0,
        "base_latency": None,
        "base_latency_ipv4": None,
        "full_check_latency": None,
        "diagnostic_vm_latency": None,
    }]


def test_build_crn_rows_skips_entries_missing_required_fields():
    # Without node_id or measured_at, the existing view emits NULL and
    # the migration's WHERE clause filters those out. The builder skips them.
    rows = _build_crn_rows("msg-3", [
        {"measured_at": 1700000000.0},                  # missing node_id
        {"node_id": "node-A"},                           # missing measured_at
        {"measured_at": 1700000001.0, "node_id": "ok"}, # valid
    ])
    assert len(rows) == 1
    assert rows[0]["node_id"] == "ok"


def test_build_crn_rows_non_numeric_field_becomes_none():
    rows = _build_crn_rows("msg-4", [
        {
            "measured_at": 1700000000.0,
            "node_id": "node-A",
            "base_latency": "not-a-number",
        },
    ])
    assert rows[0]["base_latency"] is None


def test_build_crn_rows_empty_array():
    assert _build_crn_rows("msg-5", []) == []


def test_build_ccn_rows_full_payload():
    rows = _build_ccn_rows("msg-1", [
        {
            "measured_at": 1700000000.0,
            "node_id": "node-A",
            "base_latency": 0.1,
            "base_latency_ipv4": 0.11,
            "metrics_latency": 0.2,
            "aggregate_latency": 0.3,
            "file_download_latency": 0.4,
            "pending_messages": 42,
            "eth_height_remaining": 7,
        },
    ])
    assert rows == [{
        "item_hash": "msg-1",
        "node_id": "node-A",
        "measured_at": 1700000000.0,
        "base_latency": 0.1,
        "base_latency_ipv4": 0.11,
        "metrics_latency": 0.2,
        "aggregate_latency": 0.3,
        "file_download_latency": 0.4,
        "pending_messages": 42,
        "eth_height_remaining": 7,
    }]


def test_build_ccn_rows_non_numeric_pending_messages_becomes_none():
    rows = _build_ccn_rows("msg-2", [
        {
            "measured_at": 1700000000.0,
            "node_id": "node-A",
            "pending_messages": "lots",
        },
    ])
    assert rows[0]["pending_messages"] is None

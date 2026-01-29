from dqa.anomaly.detector import detect_anomalies


def test_detect_duplicates():
    base = {
        "table": "t",
        "row_count": 100,
        "key_duplicate_rates": {"id": 0.0},
        "null_rates": {},
        "numeric_stats": {},
        "fk_violations": {},
    }
    today = {
        "table": "t",
        "row_count": 100,
        "key_duplicate_rates": {"id": 0.1},
        "null_rates": {},
        "numeric_stats": {},
        "fk_violations": {},
    }
    findings = detect_anomalies(today, base)
    assert any(f.kind == "DUPLICATE_KEY" for f in findings)


def test_detect_null_spike():
    base = {
        "table": "t",
        "row_count": 100,
        "key_duplicate_rates": {},
        "null_rates": {"c": 0.001},
        "numeric_stats": {},
        "fk_violations": {},
    }
    today = {
        "table": "t",
        "row_count": 100,
        "key_duplicate_rates": {},
        "null_rates": {"c": 0.05},
        "numeric_stats": {},
        "fk_violations": {},
    }
    findings = detect_anomalies(today, base)
    assert any(f.kind == "NULL_SPIKE" for f in findings)

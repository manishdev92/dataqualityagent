from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class Finding:
    severity: str  # "CRITICAL" | "WARN"
    table: str
    kind: str
    message: str
    details: Dict[str, Any]


def detect_anomalies(today: Dict[str, Any], base: Dict[str, Any]) -> List[Finding]:
    findings: List[Finding] = []
    t = today["table"]

    # 1) Row count drift (warn)
    rc_t = int(today.get("row_count", 0))
    rc_b = int(base.get("row_count", 0))
    if rc_b and abs(rc_t - rc_b) / rc_b > 0.30:
        findings.append(
            Finding(
                severity="WARN",
                table=t,
                kind="ROW_COUNT_DRIFT",
                message=f"Row count drift: baseline={rc_b}, today={rc_t}",
                details={"baseline": rc_b, "today": rc_t},
            )
        )

    # 2) Duplicate keys (critical)
    for k, dup_rate in today.get("key_duplicate_rates", {}).items():
        if float(dup_rate) > 0.0:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    table=t,
                    kind="DUPLICATE_KEY",
                    message=f"Duplicate rate for key '{k}' is {float(dup_rate):.4f}",
                    details={"column": k, "duplicate_rate": float(dup_rate)},
                )
            )

    # 3) Null spikes (critical)
    nr_t = today.get("null_rates", {})
    nr_b = base.get("null_rates", {})
    for c, v in nr_t.items():
        b = float(nr_b.get(c, 0.0))
        v = float(v)
        # Trigger if null rate >=2% and increased >=5x vs baseline (or baseline ~0)
        if v >= 0.02 and (b == 0.0 or v / max(b, 1e-9) >= 5.0):
            findings.append(
                Finding(
                    severity="CRITICAL",
                    table=t,
                    kind="NULL_SPIKE",
                    message=f"Null spike on '{c}': baseline={b:.4f}, today={v:.4f}",
                    details={"column": c, "baseline_null": b, "today_null": v},
                )
            )

    # 4) Numeric drift (p99 jump) (critical)
    ns_t = today.get("numeric_stats", {})
    ns_b = base.get("numeric_stats", {})
    for c, s in ns_t.items():
        if c in ns_b and "p99" in s and "p99" in ns_b[c]:
            p99_t = float(s["p99"])
            p99_b = float(ns_b[c]["p99"])
            if p99_b > 0 and p99_t / p99_b >= 5.0:
                findings.append(
                    Finding(
                        severity="CRITICAL",
                        table=t,
                        kind="P99_JUMP",
                        message=f"p99 jump on '{c}': baseline={p99_b:.2f}, today={p99_t:.2f}",
                        details={"column": c, "baseline_p99": p99_b, "today_p99": p99_t},
                    )
                )

    # 5) FK violations (critical)
    for col, info in today.get("fk_violations", {}).items():
        bad_rate = float(info.get("bad_rate", 0.0))
        if bad_rate > 0.0:
            findings.append(
                Finding(
                    severity="CRITICAL",
                    table=t,
                    kind="FK_VIOLATION",
                    message=f"FK violation on '{col}': bad_rate={bad_rate:.4f}",
                    details={"column": col, **info},
                )
            )

    return findings

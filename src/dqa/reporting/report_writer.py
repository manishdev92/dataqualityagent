from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from dqa.anomaly.detector import Finding


def write_markdown_report(
    run_id: str,
    snapshot_name: str,
    findings: List[Finding],
    profiles_today: Dict[str, Dict[str, Any]],
    out_dir: Path,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / f"run_{run_id}.md"

    by_table: Dict[str, List[Finding]] = {}
    for f in findings:
        by_table.setdefault(f.table, []).append(f)

    lines: List[str] = []
    lines.append("# DataQualityAgent Report\n\n")
    lines.append(f"- **Run ID:** `{run_id}`\n")
    lines.append(f"- **Snapshot:** `{snapshot_name}`\n")
    lines.append(f"- **Created:** {datetime.now().isoformat(timespec='seconds')}\n\n")
    lines.append("---\n\n")

    if not findings:
        lines.append("✅ No anomalies detected.\n\n")
    else:
        lines.append(f"## Findings ({len(findings)})\n\n")
        for table, flist in by_table.items():
            lines.append(f"### {table}\n\n")
            for f in flist:
                lines.append(f"- **{f.severity}** `{f.kind}` — {f.message}\n")
            lines.append("\n")

    lines.append("---\n\n")
    lines.append("## Profiles (high level)\n\n")
    for table, prof in profiles_today.items():
        lines.append(f"### {table}\n\n")
        lines.append(f"- Row count: **{prof.get('row_count')}**\n")
        lines.append(f"- Key dup rates: `{prof.get('key_duplicate_rates', {})}`\n")
        lines.append(f"- FK violations: `{prof.get('fk_violations', {})}`\n")
        lines.append(f"- Freshness: `{prof.get('freshness', {})}`\n\n")

    fp.write_text("".join(lines), encoding="utf-8")
    return fp

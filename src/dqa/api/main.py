from __future__ import annotations

from fastapi import FastAPI, HTTPException

from dqa.utils.config import PATHS

# Import run() from scripts (simple for POC)
from scripts.run_dq import run  # noqa: E402

app = FastAPI(title="DataQualityAgent", version="0.1.0")


@app.post("/dq/run")
def run_dq(target: str = "bad_day"):
    if target not in ("baseline", "bad_day"):
        raise HTTPException(status_code=400, detail="target must be baseline or bad_day")

    result = run(target)
    return result


@app.get("/dq/report/{run_id}")
def get_report(run_id: str):
    fp = PATHS.generated_reports_dir / f"run_{run_id}.md"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="report not found")
    return {"run_id": run_id, "report_markdown": fp.read_text(encoding="utf-8")}

@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs", "endpoints": ["/dq/run", "/dq/report/{run_id}"]}

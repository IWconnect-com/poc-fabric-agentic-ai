# Runbook (stub; fill from real failures)
When a run fails: 1) get the run id, 2) read the dead-letter record and reconciliation result,
3) re-run is safe (watermark only advances after reconciliation), 4) record the cause here.

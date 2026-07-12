# PulseRoute Decision Notes

This file records important project choices briefly so I can remember and explain why they were made.

## Use a TypeScript scoring package — 2026-07-12

**Decision:** Build scoring in `packages/scoring` for now and consider extracting it into a Python FastAPI service after v1 is done.
**Reason:** This preserves deterministic, explainable scoring without adding a second language, service, deployment, and network boundary before the deadline.
**Reconsider when:** The required project is complete or scoring needs Python libraries or independent scaling.

## Use PostgreSQL constraints instead of Redis locks — 2026-07-12

**Decision:** Protect assignment correctness with PostgreSQL constraints, transactions, and row locks rather than Redis distributed locks.
**Reason:** The assignment data and the rule being protected both live in PostgreSQL, so the database should enforce the rule.
**Reconsider when:** A future rule must coordinate work across independent systems that cannot share a database transaction.

## Defer SSE and frontend polish — 2026-07-12

**Decision:** Use polling for the required dashboard and postpone SSE, WebSockets, and nonessential frontend polish.
**Reason:** Backend reliability is the main goal, and polling is sufficient for the required demonstration.
**Reconsider when:** The complete August 20 Definition of Done passes.

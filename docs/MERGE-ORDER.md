# Merge order: Aquaman **before** fk-aideveloper (MM-14816)

**If you are about to merge either of these PRs, read this first.**

| | |
|---|---|
| Aquaman | `MM-14698/aquaman-batch-monitor` |
| fk-aideveloper | `MM-14628/oracle-ceiling-test` |

**Land Aquaman first. Landing fk-aideveloper first takes Ocean RCA down for the whole window
between the two merges.**

## Why

The two changes are halves of one gate, and they are coupled at *runtime*, not at build time:
`nodes.rca_report` shells out to `$FK_AIDEVELOPER_DIR/skills/ocean-rca/tools/check_rca_report.py` —
whatever is checked out there, not a pinned copy.

- **fk-aideveloper** adds the `DISCRIMINATOR:` requirement to the checker.
- **Aquaman** adds the prompt text that makes the RCA worker *write* a `DISCRIMINATOR:` line.

Measured in both directions against the real checker:

```
fk-aideveloper FIRST  (new checker, old prompt)  -> ok=False
    "missing the required `DISCRIMINATOR:` line (adjacent-mechanism-confidence hard gate)"
Aquaman FIRST         (old checker, new prompt)  -> ok=True   (clean)
```

The gate **fails closed** and `graph.py` has **no retry edge** out of `rca_report`: a refusal means
the report is not posted to Jira, `rca_done` returns `final_status="failed"`, and the completed
investigation is discarded. In the fk-aideveloper-first window that happens to *every* Ocean RCA
run. In the Aquaman-first window the old checker simply ignores the new field — no effect at all.

## What was already done to reduce the risk

`tests/test_graph.py::test_rca_report_refuses_to_post_a_report_missing_its_own_gates` skips its
DISCRIMINATOR assertion when the live checker predates the gate, so Aquaman's suite is green in the
safe order and nobody is pushed toward the dangerous one by a red build.

That is the *only* thing the skip does. It does not make fk-aideveloper-first safe — nothing in
either repo can, because the coupling is a production path. **The merge order is the control.**

## Note for reviewers

Three separate reviews of this work recommended the opposite order. Each reasoned from the *test*
(Aquaman's assertion needs the new checker) rather than from *production* (the new checker refuses
reports the old prompt produces). If you find yourself reaching the same conclusion, check which of
the two you are reasoning about.

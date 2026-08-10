#!/usr/bin/env python3
"""D1 Step 4 — does the GAN's output predict anything downstream?

THE DEPENDENT VARIABLE IS NAMED HERE, BEFORE ANY DATA IS READ, and that is the whole point of this
file. `gan-decision.md` Step 4 says: "Name the outcome first: SIT `automation_result`, rework-loop
count, or `review_findings` count." A measurement whose outcome is chosen after looking at the data
finds an effect every time — pick the metric that happens to separate, and the separation is the
choice, not the finding.

    PRIMARY   : automation_result == "passed"   (the SIT verdict; the thing the GAN exists to help)
    SECONDARY : coding_attempts               (rework loops; fewer == the scenarios were a better target)
    SECONDARY : len(review_findings)          (defects the adversarial reviewer still found)

THE PREDICTOR IS `residual_gaps`, NEVER THE VERDICT. 62.5% of real artifacts are REJECT and the
verdict vocabulary itself drifts (`APPROVE_WITH_FIXES` vs `APPROVE WITH FIXES`), so the verdict is
close to a constant and cannot separate anything — the same failure the Step 9b retrospective found
for `NEEDS_WORK` (see important-notes/MM-14816-C1-step9b-retrospective.md).

POWER, STATED UP FRONT: at a 62.5% base rate, n=10 splits ~6/4 and is underpowered for anything but
an overwhelming effect. This script therefore REFUSES to report a comparison it cannot support, and
prints what n would be needed instead. That refusal is the feature: the alternative is a number that
looks like evidence.

Prerequisite A2 (ARTIFACTS_ROOT off /tmp) is DONE, so run evidence now survives; before it, only 2 of
8 qa_scenarios logs did.

Usage:
    python scripts/gan_effect.py               # human summary
    python scripts/gan_effect.py --json        # machine-readable
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

# A comparison needs at least this many runs in EACH arm before it is reported at all.
MIN_PER_ARM = 5

OUTCOMES = {
    "primary_sit_passed": "SIT automation_result == passed",
    "secondary_coding_attempts": "rework loops (coding_attempts)",
    "secondary_review_findings": "review_findings count",
}


def _runs(artifacts_root: Path) -> list[dict]:
    """One record per execution that has BOTH a GAN artifact and a terminal run-report."""
    out: list[dict] = []
    for report_path in sorted(artifacts_root.glob("*/run-report.json")):
        try:
            rep = json.loads(report_path.read_text())
        except (OSError, ValueError):
            continue
        scen = report_path.parent / "workspace"
        gaps = rep.get("qa_gan_residual_gaps")
        if gaps is None:
            # Older runs predate the key. Recorded as UNKNOWN rather than assumed zero -- "no gaps"
            # and "we did not record gaps" are the distinction this whole queue keeps re-learning.
            arm = "unknown"
        else:
            arm = "gaps" if gaps else "clean"
        out.append({
            "execution_id": rep.get("execution_id"),
            "ticket": rep.get("ticket"),
            "arm": arm,
            "n_gaps": len(gaps) if isinstance(gaps, list) else None,
            # The PRE-REGISTERED variable. `final_status` is a fallback for runs recorded
            # before `automation_result` was written to the report, and it is a DIFFERENT
            # measure (a run can finish "completed" without the SIT passing), so it is
            # flagged rather than silently blended.
            "primary_sit_passed": (rep["automation_result"] == "passed"
                                   if rep.get("automation_result")
                                   else rep.get("final_status") == "completed"),
            "primary_is_proxy": not rep.get("automation_result"),
            "secondary_coding_attempts": rep.get("coding_attempts"),
            "secondary_review_findings": len(rep.get("review_findings") or []),
            "_scen": str(scen),
        })
    return out


def analyse(runs: list[dict]) -> dict:
    # EXCLUDED from the arms, not an abort. A run whose PRIMARY variable fell back to the
    # `final_status` proxy is measuring a DIFFERENT variable, so averaging it in produces a number
    # that reads as the pre-registered measure and is not. But aborting the whole analysis on one
    # such run is worse: `report.finish` writes `automation_result: ""` for any run that ends
    # before Station 6 (paused at a gate, aborted, RCA-only), and those runs can never acquire the
    # key however often they are re-run -- one of them would silence the analyser permanently.
    # Dropping them from the comparison keeps the remaining runs analysable and keeps the count
    # visible, so a reader can see how much of the corpus was set aside.
    comparable = [r for r in runs if not r.get("primary_is_proxy")]
    arms = {a: [r for r in comparable if r["arm"] == a] for a in ("gaps", "clean", "unknown")}
    # Runs whose PRIMARY variable had to fall back to the `final_status` proxy. `_runs` flags each
    # one; nothing read the flag, so `primary_sit_passed` silently mixed the two measures — a mean
    # over 3 real FAILs and 2 proxy PASSes reads as 0.4 and means nothing. Surfaced here, and the
    # verdict is withheld when the mix is material, because a pre-registration that quietly
    # substitutes its own outcome measure is worse than none.
    n_proxy = sum(1 for r in runs if r.get("primary_is_proxy"))
    # `n_total` counts what was READ; `n_comparable` what was analysed. Keeping both means the
    # exclusion is visible rather than silently shrinking the corpus.
    result = {
        "n_total": len(runs),
        "n_comparable": len(comparable),
        "n_by_arm": {a: len(v) for a, v in arms.items()},
        "n_primary_from_proxy": n_proxy,
        "min_per_arm": MIN_PER_ARM,
        "outcomes": OUTCOMES,
        "comparisons": {},
        "underpowered": False,
        "verdict": "",
    }
    if len(arms["gaps"]) < MIN_PER_ARM or len(arms["clean"]) < MIN_PER_ARM:
        result["underpowered"] = True
        need_g = max(0, MIN_PER_ARM - len(arms["gaps"]))
        need_c = max(0, MIN_PER_ARM - len(arms["clean"]))
        result["verdict"] = (
            f"NOT REPORTED — underpowered. Have {len(arms['gaps'])} run(s) with residual gaps and "
            f"{len(arms['clean'])} without; need {MIN_PER_ARM} in each arm. "
            f"Need {need_g} more gap-run(s) and {need_c} more clean run(s). "
            f"No comparison is printed, deliberately: a split this small produces a number that "
            f"reads as evidence and is not.")
        return result

    for key in OUTCOMES:
        g = [r[key] for r in arms["gaps"] if r[key] is not None]
        c = [r[key] for r in arms["clean"] if r[key] is not None]
        if not g or not c:
            continue
        result["comparisons"][key] = {
            "gaps_mean": round(statistics.mean(float(x) for x in g), 3),
            "clean_mean": round(statistics.mean(float(x) for x in c), 3),
            "n_gaps": len(g), "n_clean": len(c),
        }
    result["verdict"] = "reported — see comparisons (effect size only; no significance test at this n)"
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--artifacts", default="", help="artifacts root (default: config.ARTIFACTS_ROOT)")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    if a.artifacts:
        root = Path(a.artifacts)
    else:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from ocean_pipeline import config
        root = config.ARTIFACTS_ROOT

    runs = _runs(root)
    res = analyse(runs)
    res["artifacts_root"] = str(root)

    if a.json:
        print(json.dumps(res, indent=2))
        return
    print(f"GAN effect — artifacts: {root}")
    print(f"  runs with a terminal report : {res['n_total']}")
    print(f"  arms                        : {res['n_by_arm']}")
    print("  dependent variables (named BEFORE reading any data):")
    for k, v in OUTCOMES.items():
        print(f"      {k:28s} {v}")
    print()
    print(f"  {res['verdict']}")
    for key, comp in res["comparisons"].items():
        print(f"    {key}: gaps={comp['gaps_mean']} (n={comp['n_gaps']})  "
              f"clean={comp['clean_mean']} (n={comp['n_clean']})")


if __name__ == "__main__":
    main()

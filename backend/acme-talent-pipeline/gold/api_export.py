"""
Export small, chart-ready JSON summaries of the Gold tables to gold/_api/.

These files power the insights dashboard: backend/insights-api (Lambda) serves
them to the React frontend. Each file is a few KB; the logic mirrors the
analysis notebook (notebooks/acme_talent_insights.ipynb), so the dashboard and
the notebook always tell the same story.

Files written:
    meta.json         run id, as-of date, generation time
    summary.json      headline KPIs + one finding per business question
    performance.json  rating mix, transitions, department averages      (Q1)
    skills.json       critical gaps by competency/category/department   (Q2)
    promotion.json    9-box grid, candidate funnel, per-100 by dept     (Q3)
    learning.json     course status, monthly trend, top courses         (Q4)
    attrition.json    risk matrix, signals, by department, watchlist    (Q5)
    teams.json        team rating vs outcomes scatter + terciles        (Q6)
    coverage.json     competency coverage and proficiency mix           (Q7)
    extras.json       beyond-the-brief: label calibration, gap training (Q8)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd

from common.lineage import RunContext
from common.logger import get_logger
from common.storage import DataLake

log = get_logger("gold.api_export")

API_PREFIX = "gold/_api"
RATING_LABELS = {1: "Unsatisfactory", 2: "Needs Improvement", 3: "Meets", 4: "Exceeds", 5: "Outstanding"}


def _round(value: float, digits: int = 1) -> float:
    return float(round(float(value), digits))


def _records(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> list[dict]:
    out = df[columns]
    if limit is not None:
        out = out.head(limit)
    return json.loads(out.to_json(orient="records", date_format="iso"))


def performance_json(history: pd.DataFrame) -> dict[str, Any]:
    latest = history[history["is_latest_review"]].copy()
    latest["previous"] = (latest["rating"] - latest["rating_change"].fillna(0)).astype(int)
    dist = [
        {
            "rating": r,
            "label": RATING_LABELS[r],
            "previous": int((latest["previous"] == r).sum()),
            "latest": int((latest["rating"] == r).sum()),
        }
        for r in range(1, 6)
    ]
    moves = pd.crosstab(latest["previous"], latest["rating"]).reindex(index=range(1, 6), columns=range(1, 6), fill_value=0)
    by_dept = latest.groupby("department")["rating"].mean().sort_values(ascending=False)
    change = latest["rating_change"].fillna(0)
    return {
        "dist": dist,
        "transitions": moves.values.tolist(),
        "by_department": [{"department": d, "avg": _round(v, 2)} for d, v in by_dept.items()],
        "improved": int((change > 0).sum()),
        "declined": int((change < 0).sum()),
        "same": int((change == 0).sum()),
        "avg_latest": _round(latest["rating"].mean(), 2),
    }


def skills_json(gaps: pd.DataFrame) -> dict[str, Any]:
    critical = gaps[gaps["is_critical_gap"]]
    top = critical.groupby(["competency_name", "competency_category"]).size().sort_values(ascending=False).head(12)
    sev = pd.DataFrame(
        {
            "no_gap": gaps[gaps["gap"] <= 0].groupby("competency_category").size(),
            "minor": gaps[(gaps["gap"] == 1)].groupby("competency_category").size(),
            "critical": critical.groupby("competency_category").size(),
        }
    ).fillna(0)
    heat = (
        gaps.assign(crit=gaps["is_critical_gap"])
        .groupby(["department", "competency_category"])["crit"]
        .mean()
        .mul(100)
        .reset_index()
    )
    people = critical.sort_values(["gap", "employee_id"], ascending=[False, True])
    return {
        "employees_with_critical_gap": int(critical["employee_id"].nunique()),
        "critical_gaps": int(len(critical)),
        "assessments": int(len(gaps)),
        "top_gaps": [
            {"competency": name, "category": cat, "count": int(n)} for (name, cat), n in top.items()
        ],
        "severity_by_category": [
            {"category": c, "no_gap": int(r["no_gap"]), "minor": int(r["minor"]), "critical": int(r["critical"])}
            for c, r in sev.iterrows()
        ],
        "heat": [
            {"department": r["department"], "category": r["competency_category"], "pct": _round(r["crit"])}
            for _, r in heat.iterrows()
        ],
        "people": _records(
            people, ["employee_id", "full_name", "department", "job_level", "competency_name", "gap"], 15
        ),
    }


def promotion_json(pipeline: pd.DataFrame) -> dict[str, Any]:
    grid = pd.crosstab(pipeline["potential_rating"], pipeline["performance_band"])
    names = {("High", "Low"): "Enigma", ("High", "Moderate"): "High Potential", ("High", "High"): "Star",
             ("Medium", "Low"): "Inconsistent", ("Medium", "Moderate"): "Core Player", ("Medium", "High"): "High Performer",
             ("Low", "Low"): "Underperformer", ("Low", "Moderate"): "Effective", ("Low", "High"): "Trusted Pro"}
    hp, rn = pipeline["is_high_potential"], pipeline["is_ready_now"]
    top_rated = pipeline["latest_rating"] >= 4
    no_gap = pipeline["critical_gap_count"] == 0
    funnel = [
        {"stage": "All employees", "count": int(len(pipeline))},
        {"stage": "High potential", "count": int(hp.sum())},
        {"stage": "+ Ready Now", "count": int((hp & rn).sum())},
        {"stage": "+ rated 4-5", "count": int((hp & rn & top_rated).sum())},
        {"stage": "+ no critical gap", "count": int((hp & rn & top_rated & no_gap).sum())},
    ]
    cand = pipeline[pipeline["is_promotion_candidate"]]
    per100 = (cand.groupby("department").size() / pipeline.groupby("department").size() * 100).fillna(0)
    org_rate = len(cand) / len(pipeline) * 100
    return {
        "nine_box": [
            {"potential": p, "performance": b, "label": names[(p, b)], "count": int(grid.loc[p, b]) if b in grid.columns and p in grid.index else 0}
            for p in ["High", "Medium", "Low"]
            for b in ["Low", "Moderate", "High"]
        ],
        "funnel": funnel,
        "per100": sorted(
            [{"department": d, "value": _round(v), "above": bool(v >= org_rate)} for d, v in per100.items()],
            key=lambda x: -x["value"],
        ),
        "org_rate": _round(org_rate),
        "candidates": _records(
            cand.sort_values(["latest_rating", "certifications"], ascending=False),
            ["employee_id", "full_name", "department", "job_level", "latest_rating", "nine_box", "critical_gap_count", "certifications"],
            12,
        ),
        "candidates_with_gap": int((cand["critical_gap_count"] > 0).sum()),
    }


def learning_json(progress: pd.DataFrame, summary: pd.DataFrame) -> dict[str, Any]:
    status = progress["status"].value_counts().reindex(["Enrolled", "In progress", "Completed", "Certified"]).fillna(0)
    done = progress.dropna(subset=["completed_on"]).assign(month=lambda d: pd.to_datetime(d["completed_on"]).dt.to_period("M"))
    cert = progress.dropna(subset=["certified_on"]).assign(month=lambda d: pd.to_datetime(d["certified_on"]).dt.to_period("M"))
    monthly = pd.DataFrame({"completed": done.groupby("month").size(), "certified": cert.groupby("month").size()}).fillna(0)
    last_day = max(pd.to_datetime(done["completed_on"]).max(), pd.to_datetime(cert["certified_on"]).max())
    if last_day.day < 28:  # drop the partial final month so the trend does not look like a collapse
        monthly = monthly[monthly.index < last_day.to_period("M")]
    top = progress[progress["is_completed"]].groupby("course_name").size().sort_values(ascending=False).head(10)
    per_emp = summary["courses_completed"].value_counts().sort_index()
    return {
        "status": [{"status": s, "count": int(n)} for s, n in status.items()],
        "monthly": [
            {"month": str(m), "completed": int(r["completed"]), "certified": int(r["certified"])}
            for m, r in monthly.iterrows()
        ],
        "top_courses": [{"course": c, "count": int(n)} for c, n in top.items()],
        "per_employee": [{"courses": int(c), "employees": int(n)} for c, n in per_emp.items()],
        "courses_completed": int(summary["courses_completed"].sum()),
        "certifications": int(summary["certifications"].sum()),
        "median_courses": _round(summary["courses_completed"].median(), 0),
        "employees_without_courses": int((summary["courses_completed"] == 0).sum()),
    }


def attrition_json(risk: pd.DataFrame) -> dict[str, Any]:
    order = ["High", "Medium", "Low"]
    matrix = pd.crosstab(risk["derived_risk_level"], risk["hr_attrition_risk"]).reindex(index=order, columns=order[::-1], fill_value=0)
    high_hr = risk[risk["hr_attrition_risk"] == "High"]
    signals = {
        "Rating declined": risk["rating_trend"].eq("Declining"),
        "Latest rating 1-2": risk["latest_rating"] <= 2,
        "No learning in 180 days": risk["learning_events_last_180d"] == 0,
    }
    by_dept = risk.groupby("department")["is_at_risk"].mean().mul(100).sort_values(ascending=False)
    both = risk[(risk["derived_risk_level"] == "High") & (risk["hr_attrition_risk"] == "High")]
    return {
        "matrix": [
            {"derived": d, "hr": h, "count": int(matrix.loc[d, h])} for d in order for h in order[::-1]
        ],
        "signals": [
            {"signal": name, "all_pct": _round(mask.mean() * 100), "high_pct": _round(mask[high_hr.index].mean() * 100)}
            for name, mask in signals.items()
        ],
        "by_department": [{"department": d, "pct": _round(v)} for d, v in by_dept.items()],
        "at_risk_any": int(risk["is_at_risk"].sum()),
        "high_both": int(len(both)),
        "high_potential_at_risk": int((risk["is_at_risk"] & (risk["potential_rating"] == "High")).sum()),
        "watchlist": _records(
            both.sort_values(["derived_risk_score", "employee_id"], ascending=[False, True]),
            ["employee_id", "full_name", "department", "previous_rating", "latest_rating", "derived_risk_score"],
            12,
        ),
    }


def teams_json(teams: pd.DataFrame) -> dict[str, Any]:
    tp = teams[teams["has_projects"]].copy()
    r_score = tp["avg_latest_rating"].corr(tp["avg_outcome_score"])
    r_ontime = tp["avg_latest_rating"].corr(tp["on_time_rate_pct"])
    tp["band"] = pd.qcut(tp["avg_latest_rating"], 3, labels=["Lower-rated", "Middle", "Higher-rated"])
    terciles = tp.groupby("band", observed=True).agg(on_time=("on_time_rate_pct", "mean"), teams=("team_name", "count"))
    med_r, med_o = tp["avg_latest_rating"].median(), tp["avg_outcome_score"].median()
    mismatch = int(((tp["avg_latest_rating"] > med_r) & (tp["avg_outcome_score"] < med_o)).sum())
    return {
        "points": [
            {
                "team": r["team_name"], "department": r["department"],
                "rating": _round(r["avg_latest_rating"], 2), "outcome": _round(r["avg_outcome_score"]),
                "on_time": _round(r["on_time_rate_pct"]), "headcount": int(r["headcount"]),
            }
            for _, r in tp.iterrows()
        ],
        "terciles": [{"band": str(b), "on_time": _round(r["on_time"]), "teams": int(r["teams"])} for b, r in terciles.iterrows()],
        "r_outcome": _round(r_score, 2),
        "r_ontime": _round(r_ontime, 2),
        "mismatched_teams": mismatch,
        "teams_with_projects": int(len(tp)),
    }


def coverage_json(skills: pd.DataFrame) -> dict[str, Any]:
    met = skills.assign(met=skills["pct_meeting_requirement"] * skills["assessments"] / 100)
    by_comp = met.groupby(["competency_name", "competency_category"])[["met", "assessments"]].sum()
    by_comp["pct"] = by_comp["met"] / by_comp["assessments"] * 100
    levels = skills.groupby("competency_name")[[f"level_{n}_count" for n in range(1, 6)]].sum()
    comp = by_comp.join(levels).sort_values("pct")
    dept_cat = (
        met.groupby(["department", "competency_category"])[["met", "assessments"]].sum()
        .assign(pct=lambda d: d["met"] / d["assessments"] * 100)
        .reset_index()
    )
    return {
        "competencies": [
            {
                "name": name, "category": cat, "pct": _round(r["pct"]),
                "levels": [int(r[f"level_{n}_count"]) for n in range(1, 6)],
            }
            for (name, cat), r in comp.iterrows()
        ],
        "by_dept_category": [
            {"department": r["department"], "category": r["competency_category"], "pct": _round(r["pct"])}
            for _, r in dept_cat.iterrows()
        ],
    }


def extras_json(pipeline: pd.DataFrame, gaps: pd.DataFrame, progress: pd.DataFrame) -> dict[str, Any]:
    mix = []
    for label in ["Ready Now", "1-2 Years", "Not Ready"]:
        grp = pipeline[pipeline["promotion_readiness"] == label]["latest_rating"]
        if len(grp):
            mix.append({"label": label, **{f"r{r}": _round(grp.eq(r).mean() * 100) for r in range(1, 6)}})
    ready_low = int(((pipeline["promotion_readiness"] == "Ready Now") & (pipeline["latest_rating"] <= 2)).sum())
    critical = gaps[gaps["is_critical_gap"]][["employee_id", "competency_name"]]
    started = progress[["employee_id", "competency_name", "is_completed"]]
    merged = critical.merge(started, on=["employee_id", "competency_name"], how="left", indicator=True)
    agg = merged.groupby(["employee_id", "competency_name"]).agg(
        any_course=("_merge", lambda s: (s == "both").any()), done=("is_completed", lambda s: bool(s.fillna(False).any()))
    )
    total = len(agg)
    completed = int(agg["done"].sum())
    started_n = int((agg["any_course"] & ~agg["done"]).sum())
    return {
        "rating_mix_by_readiness": mix,
        "ready_now_rated_low": ready_low,
        "gap_training": [
            {"status": "No course in that skill", "count": total - completed - started_n},
            {"status": "Started, not completed", "count": started_n},
            {"status": "Completed a course", "count": completed},
        ],
        "critical_gaps_total": total,
    }


def export_api(lake: DataLake, frames: dict[str, pd.DataFrame], as_of: date, run: RunContext) -> int:
    """Build every _api JSON from the Gold frames and upload them; return the file count."""
    perf = performance_json(frames["employee_performance_history"])
    skill = skills_json(frames["employee_skill_gaps"])
    promo = promotion_json(frames["promotion_pipeline"])
    learn = learning_json(frames["employee_course_progress"], frames["employee_learning_summary"])
    attr = attrition_json(frames["attrition_risk"])
    team = teams_json(frames["team_performance_outcomes"])
    cover = coverage_json(frames["skill_distribution"])
    extra = extras_json(frames["promotion_pipeline"], frames["employee_skill_gaps"], frames["employee_course_progress"])

    try:
        report = json.loads(lake.get_bytes("_meta/dq_reports/latest/report.json"))
        ev = report["entities"]["learning_events"]
        quality = {
            "events_raw": ev["bronze_rows"], "events_clean": ev["silver_rows"],
            "events_quarantined": ev["quarantined_rows"], "events_duplicates": ev["duplicates_dropped"],
            "usable_pct": _round(ev["silver_rows"] / ev["bronze_rows"] * 100),
            "reconciles": all(report["reconciliation"].values()),
        }
    except Exception as exc:  # the dashboard must not die over a missing DQ report
        log.warning("dq_report_unavailable", extra={"error": str(exc)})
        quality = {}

    n_emp = len(frames["employee_learning_summary"])
    summary = {
        "kpis": {
            "employees": n_emp,
            "avg_rating": perf["avg_latest"],
            "promotion_candidates": promo["funnel"][3]["count"],
            "critical_gap_employees": skill["employees_with_critical_gap"],
            "high_risk_both": attr["high_both"],
            "courses_completed": learn["courses_completed"],
            "certifications": learn["certifications"],
        },
        "quality": quality,
        "findings": {
            "q1": f"{perf['improved']:,} employees improved and {perf['declined']:,} declined; the average latest rating is {perf['avg_latest']}.",
            "q2": f"{skill['employees_with_critical_gap']:,} employees are 2+ levels short of a required skill ({skill['critical_gaps']} of {skill['assessments']:,} assessments).",
            "q3": f"{promo['funnel'][3]['count']} promotion candidates; {promo['funnel'][4]['count']} could move up with no skill gap to close.",
            "q4": f"{learn['courses_completed']:,} courses completed and {learn['certifications']:,} certifications earned; {learn['employees_without_courses']} employees have none.",
            "q5": f"{attr['high_both']} employees are flagged High risk by both HR and the data; {attr['at_risk_any']} by at least one view.",
            "q6": f"Higher-rated teams deliver on time more often (r = {team['r_ontime']}); {team['mismatched_teams']} teams rate high but deliver low.",
            "q7": "Customer- and stakeholder-facing skills have the lowest coverage org-wide.",
            "q8": f"Talent labels barely track performance ({extra['ready_now_rated_low']} 'Ready Now' employees rated 1-2), and only {extra['gap_training'][2]['count']} of {extra['critical_gaps_total']} critical gaps have a completed course.",
        },
    }
    meta = {
        "run_id": run.run_id,
        "as_of_date": as_of.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    files = {
        "meta.json": meta, "summary.json": summary, "performance.json": perf, "skills.json": skill,
        "promotion.json": promo, "learning.json": learn, "attrition.json": attr, "teams.json": team,
        "coverage.json": cover, "extras.json": extra,
    }
    for name, payload in files.items():
        lake.put_bytes(f"{API_PREFIX}/{name}", json.dumps(payload, separators=(",", ":")).encode(), "application/json")
    log.info("api_export_written", extra={"files": len(files), "prefix": API_PREFIX})
    return len(files)

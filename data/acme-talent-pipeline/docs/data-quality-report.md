# Data Quality Report

Findings on the sample dataset and how the pipeline handles each one. Counts come from the
pipeline's own report (`_meta/dq_reports/latest/report.json`) and were cross-checked against
an independent profile of the raw files.

## Summary

| Entity | Bronze (raw) | Silver (clean) | Quarantined | Duplicates dropped | Reconciles |
|---|---:|---:|---:|---:|:---:|
| teams | 100 | 100 | 0 | 0 | ✅ |
| competencies | 40 | 40 | 0 | 0 | ✅ |
| employees | 2,000 | 2,000 | 0 | 0 | ✅ |
| projects | 33 | 33 | 0 | 0 | ✅ |
| performance_reviews | 4,000 | 4,000 | 0 | 0 | ✅ |
| competency_assessments | 4,000 | 4,000 | 0 | 0 | ✅ |
| talent_review | 2,000 | 2,000 | 0 | 0 | ✅ |
| **learning_events** | **29,868** | **29,536** | **182** | **150** | ✅ |

Reconciliation rule, checked on every run (the stage fails otherwise):
`bronze_rows = silver_rows + quarantined_rows + duplicates_dropped`

## Issues found in the learning-activity feed (S3 JSONL)

| # | Issue | Rows | Example | Handling (data-engineer.md §7) |
|---|---|---:|---|---|
| 1 | Truncated / malformed JSON line | 13 | `{"event_id": "EV0000015950", ..., "event_type": "quiz_attempted",` (cut off) | Row quarantined with the parser error; the rest of the file is processed; WARN logged |
| 2 | **Schema drift**: field `text` renamed to `notes` | 39 (1 whole file) | `landing/learning_activity/year=2024/month=09/day=03/events-000.jsonl` | **Whole file quarantined**: rows to `quarantine/learning_events/`, file copied to `quarantine/files/...` with a `.reason.json`; ERROR log `file_quarantined_notify_support` |
| 3 | Missing `employee_id` (`null`) | 76 | `"employee_id": null` | Row quarantined: the event cannot be attributed to a person |
| 4 | Unparseable `event_date` | 54 | `"yesterday"`, `""`, `"N/A"` | Row quarantined. The date is **not** guessed from the partition path, since a fabricated timestamp would distort activity timelines |
| 5 | Exact duplicate events | 150 (+2) | same `event_id` and identical content | First occurrence kept, the rest dropped and counted. 2 more duplicates were copies of rows already quarantined for issues 3–4, so both copies went to quarantine |
| 6 | Event date ≠ partition folder date | 0 after cleaning | — | Monitored: WARN if any valid event sits in the wrong day folder |

## Checks that passed on the HR tables (PostgreSQL)

- Required (NOT NULL) columns present and non-null.
- `CHECK` constraints: ratings 1–5, proficiency/required levels 1–5, enumerated statuses and ratings.
- Primary keys unique; foreign keys (employee→team, review→employee, assessment→employee/competency, talent→employee, project→team) all resolve.
- Valid email shape; review periods in `YYYY-H1|H2` form; project scores 0–100.

## Observations (not errors)

- `projects.csv` has one name used twice ("MediumSeaGreen Rollout") under two different `project_id`s. That's legitimate, since `project_id` is the key, but it's worth confirming with the source owner.
- Only 2 review periods and 1 talent-review cycle exist, which limits "trend" analysis (see `limitations.md`).
- `required_level` only takes the values 2, 3 and 4.

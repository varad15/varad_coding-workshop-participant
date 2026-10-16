# Source-to-Target Mapping

Required by `docs/data-engineer.md` §4: *document source-to-target mappings, identifying exactly where
columns are renamed, cast, or aggregated.*

Legend: **=** copied unchanged · **cast** type change · **rename** · **derived** computed · **lineage** added metadata

## 1. Sources → Bronze (no transformation)

| Source | Bronze target | Column handling | Code |
|---|---|---|---|
| PostgreSQL `teams`, `competencies`, `employees`, `projects`, `performance_reviews`, `competency_assessments`, `talent_review` | `bronze/hr/<table>/ingest_date=YYYY-MM-DD/batch_id=<content-hash>/` | all columns **=**, PostgreSQL types preserved (INTEGER→int32, NUMERIC(5,2)→decimal(5,2), DATE→date32, TEXT→string) | `bronze/ingest_postgres.py` |
| | | **lineage**: `_source_system`, `_source_table`, `_batch_id`, `_ingested_at`, `_run_id` | |
| S3 `landing/learning_activity/year=/month=/day=/*.jsonl` | `bronze/learning_activity/year=/month=/day=/<file>/` | each line stored **=** as `raw_line` (no parsing; malformed lines kept) | `bronze/ingest_events.py` |
| | | **lineage**: `_source_file`, `_source_size_bytes`, `_line_number`, `_batch_id`, `_ingested_at`, `_run_id` | |

## 2. Bronze → Silver

### HR tables (`silver/hr/<table>/`) — `silver/clean_hr.py`

Every contract column is kept with its source name and type (**=**). Rows are filtered, not reshaped:

| Rule | Effect | Contract |
|---|---|---|
| Latest Bronze snapshot only (`max(_ingested_at)`) | filter | — |
| Pydantic contract (NOT NULL, CHECK, enums, email shape, `YYYY-Hn` periods) | invalid → `quarantine/<table>/` | `contracts/hr.py` |
| Exact duplicate rows | dropped, counted | — |
| Same primary key, different values | all copies → quarantine | — |
| Foreign key without a Silver parent | → quarantine | `HR_FOREIGN_KEYS` |
| Lineage kept | `_source_table`, `_batch_id`, `_ingested_at` | — |

### Learning events (`silver/learning_events/event_year=/event_month=/`) — `silver/clean_events.py`

| Bronze (`raw_line` JSON key) | Silver column | Transformation |
|---|---|---|
| `event_id` | `event_id` | **=** (must match `EV` + 10 digits) |
| `employee_id` | `employee_id` | **cast** string → int; must exist in `silver/hr/employees` |
| `event_date` | `event_ts` | **cast** ISO-8601 string → timestamp (UTC) |
| `event_date` | `event_date` | **derived** `to_date(event_ts)` |
| `event_type` | `event_type` | **=** (one of 7 allowed values) |
| `course_name` | `course_name` | **=** |
| `text` | `event_text` | **rename** (`text` is too generic) |
| `score` | `score` | **cast** → int; required 0–100 for quiz events, null otherwise |
| partition path `year/month/day` | `_source_partition_date` | **derived** (lineage; compared with `event_date`) |
| — | `event_year`, `event_month` | **derived** Hive partition columns |
| `_source_file`, `_line_number`, `_batch_id`, `_ingested_at` | same | **lineage** carried from Bronze |

### Reference data — `silver/reference.py`

| Source | Target | Transformation |
|---|---|---|
| `reference/course_competency_map.csv` (in repo) | `silver/reference/course_competency_map/` | joined to `silver/hr/competencies` → adds `competency_name`, `competency_category` (rename of `name`, `category`) |

## 3. Silver → Gold (one table per business question) — `gold/*.py`

Common dimension (`gold/inputs.py`): `full_name` = **derived** `first_name || ' ' || last_name`;
`team_name` = **rename** of `teams.name`; `tenure_years` = **derived** months between `hire_date` and the as-of date / 12.
The **as-of date** is the latest date in the data (not today), so Gold is deterministic.

| Gold table | Grain | Main sources | Derived / aggregated columns |
|---|---|---|---|
| `employee_performance_history` (Q1) | employee × review_period | reviews, employees, teams | `review_sequence` (row_number), `previous_rating` (lag), `rating_change`, `is_latest_review`, `rating_label` |
| `employee_skill_gaps` (Q2) | assessment | assessments, competencies, employees | `gap = required_level − proficiency_level`; `gap_severity` (≥2 Critical, 1 Minor, ≤0 None); `is_critical_gap` |
| `promotion_pipeline` (Q3) | employee | talent_review, reviews, assessments, events | `latest_rating`, `avg_rating`, `rating_trend`, `performance_band` (1–2 Low, 3 Moderate, 4–5 High), `nine_box`, `critical_gap_count` (sum), `certifications` (count distinct), `is_promotion_candidate` |
| `employee_course_progress` (Q4) | employee × course | events, course map | `enrolled_on`/`completed_on`/`certified_on` (min date per event type), `modules_completed`, `quiz_attempts`, `quizzes_passed` (counts), `best_quiz_score` (max), `status`, `is_completed` |
| `employee_learning_summary` (Q4) | employee | course progress, events | `courses_started`, `courses_completed`, `certifications`, `quiz_attempts`, `quizzes_passed` (sums), `avg_quiz_score` (avg), `completed_courses` (collect_set) |
| `attrition_risk` (Q5) | employee | talent_review, reviews, events | four `signal_*` flags, `learning_events_last_180d`, `derived_risk_score`, `derived_risk_level`, `hr_attrition_risk` (**rename** of `attrition_risk`), `risk_views_agree`, `is_at_risk` |
| `team_performance_outcomes` (Q6) | team | employees, reviews, projects | `headcount`, `avg_latest_rating`, `pct_high_performers`, `pct_low_performers`, `avg_rating_change`, project counts by status, `avg_outcome_score`, `on_time_rate_pct` |
| `skill_distribution` (Q7) | competency × department | assessments, employees, competencies | `assessments`, `employees_assessed`, `avg_proficiency`, `avg_required_level`, `pct_meeting_requirement`, `critical_gaps`, `level_1_count` … `level_5_count` |

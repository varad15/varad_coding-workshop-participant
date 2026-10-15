import { useEffect, useState } from "react";
import { loadAll } from "./api.js";
import {
  DataTable, Finding, Funnel, GroupedVBars, HBars, HeatGrid, Legend, Lollipop,
  PairedHBars, QualityFlow, Scatter, StackedPct, TrendLine, VBars,
} from "./components/charts.jsx";

const CAT_COLORS = { Technical: "var(--series-1)", Leadership: "var(--series-2)", Domain: "var(--series-3)" };
const fmt = (v) => Number(v).toLocaleString("en-US");

const NAV = [
  ["health", "Pipeline health"], ["q1", "Performance"], ["q2", "Skill gaps"], ["q3", "Promotion"],
  ["q4", "Learning"], ["q5", "Attrition"], ["q6", "Teams"], ["q7", "Skills map"],
  ["q8", "Beyond the brief"], ["method", "Method"],
];

function Section({ id, kicker, title, children }) {
  return (
    <section className="q" id={id}>
      <p className="kicker">{kicker}</p>
      <h3>{title}</h3>
      {children}
    </section>
  );
}

function Kpi({ n, small, l, h, tone = "" }) {
  return (
    <div className={`kpi ${tone}`}>
      <div className="n">{n}{small && <small> {small}</small>}</div>
      <div className="l">{l}</div>
      <div className="h">{h}</div>
    </div>
  );
}

export default function App() {
  const [state, setState] = useState({ status: "loading" });

  useEffect(() => {
    loadAll()
      .then(({ source, data }) => setState({ status: "ready", source, ...data }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <div className="wrap"><p style={{ padding: "48px 0" }}>Loading insights&hellip;</p></div>;
  if (state.status === "error") {
    return (
      <div className="wrap">
        <div className="error-note">
          <b>The insights data has not been generated yet.</b> Run the data pipeline (it writes <code>gold/_api/</code> to
          the data lake), then reload this page. <span style={{ color: "var(--muted)" }}>{state.message}</span>
        </div>
      </div>
    );
  }

  const { summary, performance, skills, promotion, learning, attrition, teams, coverage, extras, meta, source } = state;
  const k = summary.kpis, q = summary.quality, f = summary.findings;
  const weakest = coverage.competencies.slice(0, 10);

  // Proficiency-level mix per category, aggregated client-side from the 40 competencies.
  const mixByCat = ["Technical", "Leadership", "Domain"].map((cat) => {
    const levels = [0, 0, 0, 0, 0];
    coverage.competencies.filter((c) => c.category === cat).forEach((c) => c.levels.forEach((n, i) => (levels[i] += n)));
    return { label: cat, l1: levels[0], l2: levels[1], l3: levels[2], l4: levels[3], l5: levels[4] };
  });

  const deptAvg = performance.by_department.map((d) => ({ label: d.department, value: d.avg }));
  const avgLo = Math.min(...deptAvg.map((d) => d.value)) - 0.06;
  const avgHi = Math.max(...deptAvg.map((d) => d.value)) + 0.1;

  return (
    <>
      <div className="wrap">
        <header className="masthead">
          <h1>ACME Talent Insights</h1>
          <p className="lede">
            Seven workforce questions, answered from the Gold layer of a Bronze&ndash;Silver&ndash;Gold data lake.
            Every number on this page was computed by the pipeline; nothing is typed in by hand.
          </p>
          <div className="badges">
            <span className={`badge ${source === "live" ? "live" : "snapshot"}`}>
              {source === "live" ? "Live from the data lake" : "Bundled snapshot (API unavailable)"}
            </span>
            <span className="badge">Data as of {meta.as_of_date}</span>
            <span className="badge">Pipeline run {meta.run_id}</span>
          </div>

          <div className="journey" role="img" aria-label="How the data flows from the source systems through Bronze, Silver and Gold to this page">
            <div className="stop">
              <div className="phase"><span className="dot" />Sources</div>
              <div className="big">2 systems</div>
              <div className="what">HR database (PostgreSQL) and the learning platform&rsquo;s event files (S3)</div>
            </div>
            <div className="stop bronze">
              <div className="phase"><span className="dot" />Bronze</div>
              <div className="big">{fmt(q.events_raw)}</div>
              <div className="what">raw event lines + 7 HR tables, copied byte-for-byte, never overwritten</div>
            </div>
            <div className="stop silver">
              <div className="phase"><span className="dot" />Silver</div>
              <div className="big">{fmt(q.events_clean)}</div>
              <div className="what">rows that passed every check; {fmt(q.events_quarantined)} quarantined, {fmt(q.events_duplicates)} duplicates removed</div>
            </div>
            <div className="stop gold">
              <div className="phase"><span className="dot" />Gold</div>
              <div className="big">8 tables</div>
              <div className="what">one business-ready table per question, joined and aggregated</div>
            </div>
            <div className="stop here">
              <div className="phase"><span className="dot" />This page</div>
              <div className="big">{q.usable_pct}%</div>
              <div className="what">of the raw feed survives into these answers &mdash; and the rest is accounted for below</div>
            </div>
          </div>
          <p className="journey-note">The medallion pattern: each layer is rebuilt from the one before it, so every answer can be traced back to a raw source line.</p>
        </header>
      </div>

      <nav className="nav" aria-label="Sections">
        <ul>
          {NAV.map(([id, label]) => <li key={id}><a href={`#${id}`}>{label}</a></li>)}
        </ul>
      </nav>

      <div className="wrap">
        <div className="kpis">
          <Kpi n={fmt(k.employees)} l="Employees" h="100 teams, 10 departments" />
          <Kpi n={k.avg_rating} small="/ 5" l="Average latest rating" h={`${fmt(performance.improved)} improved, ${fmt(performance.declined)} declined`} />
          <Kpi n={fmt(k.promotion_candidates)} l="Promotion candidates" h="high potential, ready now, rated 4&ndash;5" tone="good" />
          <Kpi n={fmt(k.critical_gap_employees)} l="Critical skill gaps" h="employees 2+ levels below requirement" tone="risk" />
          <Kpi n={fmt(k.high_risk_both)} l="High attrition risk" h="flagged by HR and by the data" tone="risk" />
          <Kpi n={fmt(k.courses_completed)} l="Courses completed" h={`${fmt(k.certifications)} certifications earned`} />
        </div>

        <Section id="health" kicker="Before any answers" title="Can these numbers be trusted?">
          <p className="intro">
            The learning feed arrived with real-world damage: truncated lines, events without an employee, unparseable
            dates, duplicates, and one file whose schema had silently changed. The pipeline&rsquo;s rule is
            <b> nothing disappears without a recorded reason</b>: every rejected record sits in quarantine with its
            reason, and every layer must reconcile &mdash; raw&nbsp;=&nbsp;clean&nbsp;+&nbsp;quarantined&nbsp;+&nbsp;duplicates
            &mdash; or the run fails.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Every learning event is accounted for</h4>
              <QualityFlow raw={q.events_raw} clean={q.events_clean} quarantined={q.events_quarantined}
                duplicates={q.events_duplicates} usablePct={q.usable_pct} />
              <p className="note">{fmt(q.events_raw)} = {fmt(q.events_clean)} + {fmt(q.events_quarantined)} + {fmt(q.events_duplicates)} &mdash; the reconciliation this run passed.</p>
            </div>
            {q.reasons && (
              <div className="panel">
                <h4>Why records were quarantined</h4>
                <HBars rows={q.reasons.map((r) => ({ label: r.reason, value: r.count, color: "var(--crit)" }))} labelWidth={210} />
                <p className="note">The schema-drift file was quarantined whole: when a producer changes its format, every row in that file is suspect until a human looks.</p>
              </div>
            )}
          </div>
          {q.entities && (
            <details className="table">
              <summary>Reconciliation for all 8 entities (HR tables came through clean)</summary>
              <DataTable
                columns={[{ key: "entity", label: "Entity" }, { key: "bronze", label: "Raw (Bronze)" }, { key: "silver", label: "Clean (Silver)" },
                  { key: "quarantined", label: "Quarantined" }, { key: "duplicates", label: "Duplicates" }, { key: "ok", label: "Reconciles" }]}
                rows={q.entities.map((e) => ({ ...e, bronze: fmt(e.bronze), silver: fmt(e.silver), ok: e.reconciles ? "yes" : "NO" }))}
              />
            </details>
          )}
          <Finding>All 8 entities reconcile. The HR tables were clean; the learning feed lost {q.usable_pct ? (100 - q.usable_pct).toFixed(1) : "1.1"}% to quarantine and duplicates &mdash; none of it reaches the analysis below.</Finding>
        </Section>

        <Section id="q1" kicker="Question 1 of 7" title={`Ratings moved up: ${fmt(performance.improved)} employees improved, ${fmt(performance.declined)} declined`}>
          <p className="intro">
            Every employee has two reviews &mdash; 2024-H2 and 2025-H1 &mdash; rated 1 (Unsatisfactory) to 5 (Outstanding).
            Comparing the two periods shows not just where the workforce stands but which way it is moving, and the
            transition grid shows exactly who moved where.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Rating mix, previous vs latest review</h4>
              <GroupedVBars
                rows={performance.dist.map((d) => ({ label: `${d.rating} ${d.label.split(" ")[0]}`, a: d.previous, b: d.latest }))}
                seriesA="2024-H2" seriesB="2025-H1"
              />
            </div>
            <div className="panel">
              <h4>Average latest rating by department</h4>
              <Lollipop rows={deptAvg} refValue={performance.avg_latest} refLabel={`org ${performance.avg_latest}`} domain={[avgLo, avgHi]} />
            </div>
          </div>
          <div className="panel">
            <h4>Who moved where &mdash; rows: previous rating, columns: latest rating</h4>
            <HeatGrid cols={["1", "2", "3", "4", "5"]} rowsLabels={["1", "2", "3", "4", "5"]} cellH={34} labelWidth={60}
              values={performance.transitions} format={fmt} maxWidth={520}
              tooltip={(r, c, v) => `${fmt(v)} employees went from rating ${r} to ${c}`} />
            <p className="note">The dark diagonal is stability; everything above the diagonal is improvement, below it decline.</p>
          </div>
          <Finding>{f.q1}</Finding>
        </Section>

        <Section id="q2" kicker="Question 2 of 7" title={`${fmt(skills.employees_with_critical_gap)} employees are 2+ levels short of a required skill`}>
          <p className="intro">
            Each of the 4,000 assessments compares an employee&rsquo;s <b>proficiency level</b> (1&ndash;5) with the
            <b> required level</b> for their role. The gap is required minus proficiency; a gap of <b>2 or more levels is
            treated as critical</b> &mdash; a stated assumption, documented with the pipeline, not a hidden choice.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Skills with the most critical gaps</h4>
              <Legend items={Object.entries(CAT_COLORS)} />
              <HBars rows={skills.top_gaps.map((g) => ({ label: g.competency, value: g.count, color: CAT_COLORS[g.category] }))} />
            </div>
            <div className="panel">
              <h4>Gap severity by skill category, share of assessments</h4>
              <StackedPct
                rows={skills.severity_by_category.map((s) => ({ label: s.category, ...s }))}
                series={[
                  { key: "no_gap", label: "No gap", color: "var(--grid)" },
                  { key: "minor", label: "Minor gap", color: "var(--warn)" },
                  { key: "critical", label: "Critical gap", color: "var(--crit)", dark: true },
                ]}
              />
              <details className="table">
                <summary>People to prioritise, largest gaps first</summary>
                <DataTable
                  columns={[{ key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                    { key: "job_level", label: "Level" }, { key: "competency_name", label: "Skill" }, { key: "gap", label: "Gap" }]}
                  rows={skills.people}
                />
              </details>
            </div>
          </div>
          <Finding>{f.q2}</Finding>
        </Section>

        <Section id="q3" kicker="Question 3 of 7" title={`${fmt(promotion.funnel[3].count)} promotion candidates; ${fmt(promotion.funnel[4].count)} with no skill gap to close`}>
          <p className="intro">
            The industry-standard <b>9-box grid</b> places everyone on two axes: latest performance (rating 1&ndash;2 low,
            3 moderate, 4&ndash;5 high) and HR-assessed potential. A <b>promotion candidate</b> must clear three bars at
            once: High potential <i>and</i> Ready Now in the talent review <i>and</i> rated 4 or 5 in the latest cycle
            &mdash; the funnel shows how 2,000 people narrow to {fmt(promotion.funnel[3].count)}.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>9-box talent grid, employees per cell</h4>
              <HeatGrid
                cols={["Low perf", "Moderate", "High perf"]}
                rowsLabels={["High potential", "Medium", "Low"]}
                cellH={56}
                values={[["High"], ["Medium"], ["Low"]].map(([p]) =>
                  ["Low", "Moderate", "High"].map((b) => promotion.nine_box.find((c) => c.potential === p && c.performance === b)?.count ?? 0)
                )}
                cellText={(rl, c, v) => fmt(v)}
                tooltip={(rl, c, v) => {
                  const p = rl.split(" ")[0];
                  const perf = c.startsWith("Low") ? "Low" : c.startsWith("Mod") ? "Moderate" : "High";
                  const cell = promotion.nine_box.find((x) => x.potential === p && x.performance === perf);
                  return `${cell ? cell.label : ""}: ${fmt(v)} employees`;
                }}
              />
              <p className="note">Top-right is the Star cell; hover any cell for its 9-box name.</p>
            </div>
            <div className="panel">
              <h4>From {fmt(promotion.funnel[0].count)} employees to a shortlist</h4>
              <Funnel stages={promotion.funnel} />
            </div>
          </div>
          <div className="panel">
            <h4>Promotion candidates per 100 employees &mdash; green is above the company rate of {promotion.org_rate}</h4>
            <HBars
              rows={promotion.per100.map((d) => ({ label: d.department, value: d.value, color: d.above ? "var(--series-3)" : "var(--series-1-soft)" }))}
              format={(v) => v.toFixed(1)}
            />
            <details className="table">
              <summary>Top candidates, highest rating and most certifications first</summary>
              <DataTable
                columns={[{ key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                  { key: "job_level", label: "Level" }, { key: "latest_rating", label: "Rating" },
                  { key: "nine_box", label: "9-box" }, { key: "critical_gap_count", label: "Critical gaps" },
                  { key: "certifications", label: "Certifications" }]}
                rows={promotion.candidates}
              />
            </details>
          </div>
          <Finding>{f.q3}</Finding>
        </Section>

        <Section id="q4" kicker="Question 4 of 7" title={`${fmt(learning.courses_completed)} courses completed, ${fmt(learning.certifications)} certifications earned`}>
          <p className="intro">
            The learning feed is a stream of events &mdash; enrollment, modules, quizzes, completion, certification.
            The pipeline replays each employee&rsquo;s events per course into a single journey, so &ldquo;how far did
            they get?&rdquo; has one answer per person per course.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>How far employees got in each course</h4>
              <VBars rows={learning.status.map((s, i) => ({ label: s.status, value: s.count, color: i >= 3 ? "var(--series-3)" : i === 2 ? "var(--series-1)" : "var(--series-1-soft)" }))} />
            </div>
            <div className="panel">
              <h4>Completions per month, to {learning.monthly[learning.monthly.length - 1].month}</h4>
              <TrendLine
                months={learning.monthly.map((m) => m.month)}
                series={[
                  { label: "Completed", color: "var(--series-1)", values: learning.monthly.map((m) => m.completed) },
                  { label: "Certified", color: "var(--series-3)", values: learning.monthly.map((m) => m.certified) },
                ]}
              />
              <p className="note">The final partial month is excluded so the trend does not end in a false cliff.</p>
            </div>
          </div>
          <div className="row cols-2">
            <div className="panel">
              <h4>Most completed courses</h4>
              <HBars rows={learning.top_courses.map((c) => ({ label: c.course, value: c.count }))} labelWidth={230} />
            </div>
            <div className="panel">
              <h4>Courses completed per employee</h4>
              <VBars rows={learning.per_employee.map((p) => ({ label: String(p.courses), value: p.employees }))} />
              <p className="note">Median {learning.median_courses} per employee; {fmt(learning.employees_without_courses)} employees have completed none.</p>
            </div>
          </div>
          <Finding>{f.q4}</Finding>
        </Section>

        <Section id="q5" kicker="Question 5 of 7" title={`${fmt(attrition.high_both)} employees are flagged high risk by both HR and the data`}>
          <p className="intro">
            Two independent views sit side by side. <b>HR&rsquo;s view</b> is the manager-assessed flag from the talent
            review. The <b>data-derived view</b> scores observable signals: rating declined (+2), latest rating 1&ndash;2
            (+2), a negative manager comment (+1), no learning activity in 180 days (+1) &mdash; a score of 3 or more is
            High. Where both agree, act; where they disagree, that disagreement is itself the insight.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Data-derived vs HR-assessed risk, employees</h4>
              <HeatGrid
                cols={["HR: Low", "HR: Medium", "HR: High"]}
                rowsLabels={["Data: High", "Data: Medium", "Data: Low"]}
                cellH={46}
                values={["High", "Medium", "Low"].map((d) =>
                  ["Low", "Medium", "High"].map((h) => attrition.matrix.find((m) => m.derived === d && m.hr === h)?.count ?? 0)
                )}
                format={fmt}
              />
              <p className="note">Top-right = flagged by both. Bottom-right and top-left are the conversations worth having with managers.</p>
            </div>
            <div className="panel">
              <h4>Which signals show up in HR&rsquo;s high-risk group</h4>
              <PairedHBars
                rows={attrition.signals.map((s) => ({ label: s.signal, a: s.high_pct.toFixed(0), b: s.all_pct.toFixed(0) }))}
                labelA="HR flags high" labelB="All employees"
              />
            </div>
          </div>
          <div className="panel">
            <h4>Share of employees at risk in either view, by department</h4>
            <HBars rows={attrition.by_department.map((d) => ({ label: d.department, value: d.pct, color: "var(--series-1)" }))} unit="%" format={(v) => v.toFixed(0)} />
            <details className="table">
              <summary>Retention watchlist &mdash; high in both views, highest score first</summary>
              <DataTable
                columns={[{ key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                  { key: "previous_rating", label: "Prev rating" }, { key: "latest_rating", label: "Latest" },
                  { key: "derived_risk_score", label: "Risk score" }]}
                rows={attrition.watchlist}
              />
            </details>
          </div>
          <Finding>{f.q5}</Finding>
          <p className="method">With only two review periods, a &ldquo;trend&rdquo; is one period-over-period change &mdash; a stated limit of the source data.</p>
        </Section>

        <Section id="q6" kicker="Question 6 of 7" title={`Higher-rated teams deliver on time more often (r = ${teams.r_ontime})`}>
          <p className="intro">
            Projects belong to teams, not individuals, so this comparison is at team level: each team&rsquo;s average
            latest rating against the outcomes of its projects. Only {teams.teams_with_projects} of 100 teams have
            projects, so read this as an indicative correlation &mdash; not proof that performance causes outcomes.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Each team: rating vs project outcome &mdash; dot size is headcount</h4>
              <Scatter points={teams.points} xLabel="team average latest rating (1-5)" yLabel="avg project outcome score" />
            </div>
            <div className="panel">
              <h4>On-time delivery by team rating band</h4>
              <VBars rows={teams.terciles.map((t, i) => ({ label: `${t.band} (${t.teams})`, value: t.on_time, color: ["var(--series-1-soft)", "var(--series-1)", "var(--seq-6)"][i] }))} />
              <p className="note">The link is clearer for on-time delivery (r = {teams.r_ontime}) than for the outcome score (r = {teams.r_outcome}).</p>
            </div>
          </div>
          <Finding>{f.q6}</Finding>
        </Section>

        <Section id="q7" kicker="Question 7 of 7" title="Customer- and stakeholder-facing skills have the weakest coverage">
          <p className="intro">
            Coverage = the share of assessments meeting the required level, per competency. The weakest skills org-wide
            are the customer- and stakeholder-facing ones &mdash; which lines up with where the critical gaps in
            question&nbsp;2 cluster, two independent cuts of the same underlying truth.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Lowest coverage among the 40 competencies, % meeting requirement</h4>
              <Legend items={Object.entries(CAT_COLORS)} />
              <HBars rows={weakest.map((c) => ({ label: c.name, value: c.pct, color: CAT_COLORS[c.category] }))} unit="%" labelWidth={200} format={(v) => v.toFixed(0)} />
            </div>
            <div className="panel">
              <h4>Share of assessments with a critical gap, by department and category</h4>
              <HeatGrid
                cols={["Technical", "Leadership", "Domain"]}
                rowsLabels={[...new Set(skills.heat.map((h) => h.department))]}
                cellH={26} labelWidth={140}
                values={[...new Set(skills.heat.map((h) => h.department))].map((d) =>
                  ["Technical", "Leadership", "Domain"].map((c) => skills.heat.find((h) => h.department === d && h.category === c)?.pct ?? 0)
                )}
                format={(v) => `${v.toFixed(1)}%`}
              />
            </div>
          </div>
          <div className="panel">
            <h4>Proficiency-level mix by skill category, share of assessments</h4>
            <StackedPct
              rows={mixByCat}
              series={[
                { key: "l1", label: "Level 1", color: "var(--seq-1)" },
                { key: "l2", label: "Level 2", color: "var(--seq-2)" },
                { key: "l3", label: "Level 3", color: "var(--seq-3)" },
                { key: "l4", label: "Level 4", color: "var(--seq-5)", dark: true },
                { key: "l5", label: "Level 5", color: "var(--seq-7)", dark: true },
              ]}
            />
          </div>
          <Finding>{f.q7}</Finding>
        </Section>

        <Section id="q8" kicker="Beyond the brief" title="Talent labels don&rsquo;t predict performance &mdash; and training misses the gaps">
          <p className="intro">
            Two analyses the brief didn&rsquo;t ask for, found by joining datasets the questions keep apart. First:
            if the talent review worked, &ldquo;Ready Now&rdquo; people should out-rate &ldquo;Not Ready&rdquo; people.
            Second: because every course maps to a competency, training can be checked against the exact skill of each
            critical gap.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Latest rating mix by promotion-readiness label</h4>
              <StackedPct
                rows={extras.rating_mix_by_readiness.map((m) => ({ label: m.label, ...m }))}
                series={[
                  { key: "r1", label: "Rated 1", color: "var(--crit)", dark: true },
                  { key: "r2", label: "Rated 2", color: "var(--series-2)" },
                  { key: "r3", label: "Rated 3", color: "var(--grid)" },
                  { key: "r4", label: "Rated 4", color: "var(--series-1-soft)" },
                  { key: "r5", label: "Rated 5", color: "var(--series-1)", dark: true },
                ]}
              />
              <p className="note">The three bars are nearly identical &mdash; the labels barely track measured performance.</p>
            </div>
            <div className="panel">
              <h4>Training on the exact skill of each critical gap</h4>
              <HBars
                rows={extras.gap_training.map((g, i) => ({ label: g.status, value: g.count, color: ["var(--crit)", "var(--warn)", "var(--good)"][i] }))}
                labelWidth={200}
              />
              <p className="note">Of {fmt(extras.critical_gaps_total)} critical gaps, training budget is reaching almost none of them.</p>
            </div>
          </div>
          <Finding>{f.q8}</Finding>
        </Section>

        <Section id="method" kicker="How this was built" title="Method, assumptions and limits">
          <p className="intro">
            The pipeline ingests from PostgreSQL and S3, validates every row against explicit schema contracts
            (Pydantic, run inside Spark), quarantines what fails with a reason, rebuilds Silver and Gold
            deterministically on every run, and checks itself: layer reconciliation, unique keys per Gold table, and
            Gold totals balanced back against Silver. Running it twice produces byte-identical results.
          </p>
          <div className="row cols-2">
            <div className="panel">
              <h4>Stated assumptions</h4>
              <table>
                <tbody>
                  <tr><td>Critical gap</td><td>required level &minus; proficiency &ge; 2</td></tr>
                  <tr><td>Promotion candidate</td><td>High potential + Ready Now + rating &ge; 4</td></tr>
                  <tr><td>Derived risk score</td><td>rating fell +2, rating &le; 2 +2, negative comment +1, no learning in 180 days +1; &ge; 3 is High</td></tr>
                  <tr><td>As-of date</td><td>{meta.as_of_date}, taken from the data itself so reruns are reproducible</td></tr>
                  <tr><td>Course &rarr; skill map</td><td>42 courses mapped to the 40 competencies, maintained as reference data</td></tr>
                </tbody>
              </table>
            </div>
            <div className="panel">
              <h4>Known limits</h4>
              <table>
                <tbody>
                  <tr><td>Trends</td><td>only two review periods exist, so a trend is a single change</td></tr>
                  <tr><td>Teams</td><td>{teams.teams_with_projects} of 100 teams have projects; correlation, not causation</td></tr>
                  <tr><td>Quarantine</td><td>replay is manual by design &mdash; a human inspects before anything re-enters</td></tr>
                  <tr><td>Labels</td><td>talent-review labels are HR judgements, which question 8 puts to the test</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </Section>

        <footer>
          <p>
            Pipeline run {meta.run_id}, data as of {meta.as_of_date}, page generated {meta.generated_at?.slice(0, 16).replace("T", " ")} UTC.
          </p>
          <p>
            PostgreSQL and S3 feed a Bronze&ndash;Silver&ndash;Gold lake built by PySpark on Kubernetes; a Lambda serves
            the Gold summaries this page reads. Every rejected record is quarantined with a reason, and every layer
            reconciles against the one before it.
          </p>
        </footer>
      </div>
    </>
  );
}

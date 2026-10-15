import { useEffect, useState } from "react";
import { loadAll } from "./api.js";
import {
  DataTable, Finding, Funnel, GroupedVBars, HBars, HeatGrid, KpiTile, Legend,
  PairedHBars, Scatter, StackedPct, TrendLine, VBars,
} from "./components/charts.jsx";

const CAT_COLORS = { Technical: "var(--series-1)", Leadership: "var(--series-2)", Domain: "var(--series-3)" };
const fmt = (v) => Number(v).toLocaleString("en-US");

function Section({ no, title, children }) {
  return (
    <section className="q">
      <h2>{no}</h2>
      <h3>{title}</h3>
      {children}
    </section>
  );
}

export default function App() {
  const [state, setState] = useState({ status: "loading" });

  useEffect(() => {
    loadAll()
      .then(({ source, data }) => setState({ status: "ready", source, ...data }))
      .catch((err) => setState({ status: "error", message: String(err) }));
  }, []);

  if (state.status === "loading") return <p style={{ padding: 40 }}>Loading insights…</p>;
  if (state.status === "error") {
    return (
      <div className="error-note">
        <b>Could not load the insights data.</b> Run the data pipeline first (it writes <code>gold/_api/</code> to the
        data lake), then redeploy the frontend. <span style={{ color: "var(--muted)" }}>{state.message}</span>
      </div>
    );
  }

  const { summary, performance, skills, promotion, learning, attrition, teams, coverage, extras, meta, source } = state;
  const k = summary.kpis, q = summary.quality, f = summary.findings;
  const weakest = coverage.competencies.slice(0, 10);

  return (
    <>
      <header className="masthead">
        <h1>ACME Talent Insights</h1>
        <p className="sub">Seven workforce questions, answered live from the Gold layer of the data lake.</p>
        <div className="badges">
          <span className={`badge ${source === "live" ? "live" : "snapshot"}`}>
            {source === "live" ? "live from the data lake" : "bundled snapshot (API unavailable)"}
          </span>
          <span className="badge">as of {meta.as_of_date}</span>
          <span className="badge">pipeline run {meta.run_id}</span>
          {q.reconciles && <span className="badge">all layers reconcile · {q.usable_pct}% of events usable</span>}
        </div>
        <div className="tiles">
          <KpiTile label="Employees" value={fmt(k.employees)} hint="100 teams, 10 departments" />
          <KpiTile label="Average latest rating" value={`${k.avg_rating} / 5`} hint={`${fmt(performance.improved)} improved, ${fmt(performance.declined)} declined`} />
          <KpiTile label="Promotion candidates" value={fmt(k.promotion_candidates)} hint="High potential + Ready Now + rated 4-5" tone="good" />
          <KpiTile label="Critical skill gaps" value={fmt(k.critical_gap_employees)} hint="employees 2+ levels below requirement" tone="risk" />
          <KpiTile label="High attrition risk" value={fmt(k.high_risk_both)} hint="flagged High by HR and by the data" tone="risk" />
          <KpiTile label="Courses completed" value={fmt(k.courses_completed)} hint={`${fmt(k.certifications)} certifications earned`} />
        </div>
      </header>

      <Section no="Question 1 · Performance ratings & review history" title={`Ratings moved up: ${fmt(performance.improved)} employees improved, ${fmt(performance.declined)} declined`}>
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
            <HBars rows={performance.by_department.map((d) => ({ label: d.department, value: d.avg }))} format={(v) => v.toFixed(2)} />
          </div>
        </div>
        <Finding>{f.q1}</Finding>
      </Section>

      <Section no="Question 2 · Critical skill gaps" title={`${fmt(skills.employees_with_critical_gap)} employees are 2+ levels short of a required skill`}>
        <div className="row cols-2">
          <div className="panel">
            <h4>Skills with the most critical gaps</h4>
            <Legend items={Object.entries(CAT_COLORS)} />
            <HBars rows={skills.top_gaps.map((g) => ({ label: g.competency, value: g.count, color: CAT_COLORS[g.category] }))} />
          </div>
          <div className="panel">
            <h4>Gap severity by skill category (% of assessments)</h4>
            <StackedPct
              rows={skills.severity_by_category.map((s) => ({ label: s.category, ...s }))}
              series={[
                { key: "no_gap", label: "No gap", color: "var(--grid)" },
                { key: "minor", label: "Minor gap", color: "var(--warn)" },
                { key: "critical", label: "Critical gap", color: "var(--crit)", dark: true },
              ]}
            />
            <details className="table">
              <summary>People to prioritise (largest gaps first)</summary>
              <DataTable
                columns={[
                  { key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                  { key: "job_level", label: "Level" }, { key: "competency_name", label: "Skill" }, { key: "gap", label: "Gap" },
                ]}
                rows={skills.people}
              />
            </details>
          </div>
        </div>
        <Finding>{f.q2}</Finding>
      </Section>

      <Section no="Question 3 · Ready for promotion" title={`${fmt(promotion.funnel[3].count)} promotion candidates; ${fmt(promotion.funnel[4].count)} with no skill gap to close`}>
        <div className="row cols-2">
          <div className="panel">
            <h4>9-box talent grid (employees)</h4>
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
                const cell = promotion.nine_box.find((x) => x.potential === p && `${x.performance}` === c.split(" ")[0].replace("perf", "").trim());
                return `${cell ? cell.label : ""}: ${fmt(v)} employees`;
              }}
            />
          </div>
          <div className="panel">
            <h4>From {fmt(promotion.funnel[0].count)} employees to a shortlist</h4>
            <Funnel stages={promotion.funnel} />
          </div>
        </div>
        <div className="panel" style={{ marginTop: 18 }}>
          <h4>Promotion candidates per 100 employees (green = above company rate {promotion.org_rate})</h4>
          <HBars
            rows={promotion.per100.map((d) => ({ label: d.department, value: d.value, color: d.above ? "var(--series-3)" : "var(--series-1-soft)" }))}
            format={(v) => v.toFixed(1)}
          />
          <details className="table">
            <summary>Top candidates (highest rating, most certifications)</summary>
            <DataTable
              columns={[
                { key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                { key: "job_level", label: "Level" }, { key: "latest_rating", label: "Rating" },
                { key: "nine_box", label: "9-box" }, { key: "critical_gap_count", label: "Critical gaps" },
                { key: "certifications", label: "Certifications" },
              ]}
              rows={promotion.candidates}
            />
          </details>
        </div>
        <Finding>{f.q3}</Finding>
      </Section>

      <Section no="Question 4 · Training & development" title={`${fmt(learning.courses_completed)} courses completed, ${fmt(learning.certifications)} certifications earned`}>
        <div className="row cols-2">
          <div className="panel">
            <h4>How far employees got in each course</h4>
            <VBars rows={learning.status.map((s, i) => ({ label: s.status, value: s.count, color: i >= 3 ? "var(--series-3)" : i === 2 ? "var(--series-1)" : "var(--series-1-soft)" }))} />
          </div>
          <div className="panel">
            <h4>Completions per month (to {learning.monthly[learning.monthly.length - 1].month})</h4>
            <TrendLine
              months={learning.monthly.map((m) => m.month)}
              series={[
                { label: "Completed", color: "var(--series-1)", values: learning.monthly.map((m) => m.completed) },
                { label: "Certified", color: "var(--series-3)", values: learning.monthly.map((m) => m.certified) },
              ]}
            />
          </div>
        </div>
        <div className="panel" style={{ marginTop: 18 }}>
          <h4>Most completed courses</h4>
          <HBars rows={learning.top_courses.map((c) => ({ label: c.course, value: c.count }))} labelWidth={230} />
        </div>
        <Finding>{f.q4}</Finding>
      </Section>

      <Section no="Question 5 · Attrition risk" title={`${fmt(attrition.high_both)} employees are flagged High risk by both HR and the data`}>
        <div className="row cols-2">
          <div className="panel">
            <h4>Data-derived vs HR-assessed risk (employees)</h4>
            <HeatGrid
              cols={["HR: Low", "HR: Medium", "HR: High"]}
              rowsLabels={["Data: High", "Data: Medium", "Data: Low"]}
              cellH={46}
              values={["High", "Medium", "Low"].map((d) =>
                ["Low", "Medium", "High"].map((h) => attrition.matrix.find((m) => m.derived === d && m.hr === h)?.count ?? 0)
              )}
              format={fmt}
            />
          </div>
          <div className="panel">
            <h4>Which signals show up in HR's high-risk group</h4>
            <PairedHBars
              rows={attrition.signals.map((s) => ({ label: s.signal, a: s.high_pct.toFixed(0), b: s.all_pct.toFixed(0) }))}
              labelA="HR flags High" labelB="All employees"
            />
          </div>
        </div>
        <div className="panel" style={{ marginTop: 18 }}>
          <h4>Share of employees at risk (either view), by department</h4>
          <HBars rows={attrition.by_department.map((d) => ({ label: d.department, value: d.pct, color: "var(--series-1)" }))} unit="%" format={(v) => v.toFixed(0)} />
          <details className="table">
            <summary>Retention watchlist (High in both views, highest score first)</summary>
            <DataTable
              columns={[
                { key: "full_name", label: "Employee" }, { key: "department", label: "Department" },
                { key: "previous_rating", label: "Prev rating" }, { key: "latest_rating", label: "Latest" },
                { key: "derived_risk_score", label: "Risk score" },
              ]}
              rows={attrition.watchlist}
            />
          </details>
        </div>
        <Finding>{f.q5}</Finding>
      </Section>

      <Section no="Question 6 · Performance vs team outcomes" title={`Higher-rated teams deliver on time more often (r = ${teams.r_ontime})`}>
        <div className="row cols-2">
          <div className="panel">
            <h4>Each team: rating vs project outcome (size = headcount)</h4>
            <Scatter points={teams.points} xLabel="team average latest rating (1-5)" yLabel="avg project outcome score" />
          </div>
          <div className="panel">
            <h4>On-time delivery by team rating band</h4>
            <VBars rows={teams.terciles.map((t, i) => ({ label: `${t.band} (${t.teams})`, value: t.on_time, color: ["var(--series-1-soft)", "var(--series-1)", "var(--seq-6)"][i] }))} />
          </div>
        </div>
        <Finding>{f.q6} (n = {teams.teams_with_projects} teams with projects; correlation, not causation.)</Finding>
      </Section>

      <Section no="Question 7 · Skill distribution" title="Customer- and stakeholder-facing skills have the weakest coverage">
        <div className="row cols-2">
          <div className="panel">
            <h4>Lowest coverage of the 40 competencies (% meeting requirement)</h4>
            <Legend items={Object.entries(CAT_COLORS)} />
            <HBars rows={weakest.map((c) => ({ label: c.name, value: c.pct, color: CAT_COLORS[c.category] }))} unit="%" labelWidth={200} format={(v) => v.toFixed(0)} />
          </div>
          <div className="panel">
            <h4>Share of assessments with a critical gap, by department &amp; category</h4>
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
        <Finding>{f.q7}</Finding>
      </Section>

      <Section no="Beyond the brief" title="Talent labels don't predict performance - and training misses the gaps">
        <div className="row cols-2">
          <div className="panel">
            <h4>Latest rating mix by promotion-readiness label</h4>
            <StackedPct
              rows={extras.rating_mix_by_readiness.map((m) => ({ label: m.label, ...m }))}
              series={[
                { key: "r1", label: "rated 1", color: "var(--crit)", dark: true },
                { key: "r2", label: "rated 2", color: "var(--series-2)" },
                { key: "r3", label: "rated 3", color: "var(--grid)" },
                { key: "r4", label: "rated 4", color: "var(--series-1-soft)" },
                { key: "r5", label: "rated 5", color: "var(--series-1)", dark: true },
              ]}
            />
          </div>
          <div className="panel">
            <h4>Training on the exact skill of each critical gap</h4>
            <HBars
              rows={extras.gap_training.map((g, i) => ({ label: g.status, value: g.count, color: ["var(--crit)", "var(--warn)", "var(--good)"][i] }))}
              labelWidth={200}
            />
          </div>
        </div>
        <Finding>{f.q8}</Finding>
      </Section>

      <footer>
        Data: PostgreSQL (HR system) + S3 learning events → Bronze → Silver → Gold on the data lake · every rejected
        record is quarantined with a reason ({q.events_quarantined} quarantined, {q.events_duplicates} duplicates
        removed, {q.usable_pct}% usable) · generated {meta.generated_at?.slice(0, 16).replace("T", " ")} UTC
      </footer>
    </>
  );
}

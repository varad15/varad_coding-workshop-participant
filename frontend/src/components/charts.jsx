// Small SVG chart kit for the dashboard.
// Mark conventions (per the data-viz method): thin marks with a 4px rounded
// data-end anchored to the baseline, 2px gaps between adjacent fills, values
// direct-labeled in ink (never the series color), recessive grid, native
// tooltips on every mark, and legends whenever two or more series share a plot.

const fmt = (v) => Number(v).toLocaleString("en-US");
const svgStyle = (maxWidth) => ({ width: "100%", height: "auto", maxWidth, display: "block" });

/** Bar with only the data-end rounded (horizontal: right end; vertical: top). */
function barPath(x, y, w, h, r, horizontal) {
  if (w <= 0 || h <= 0) return "";
  const rr = Math.min(r, horizontal ? w : h, h / 2, w / 2);
  return horizontal
    ? `M${x},${y} h${w - rr} a${rr},${rr} 0 0 1 ${rr},${rr} v${h - 2 * rr} a${rr},${rr} 0 0 1 -${rr},${rr} h-${w - rr} z`
    : `M${x},${y + rr} a${rr},${rr} 0 0 1 ${rr},-${rr} h${w - 2 * rr} a${rr},${rr} 0 0 1 ${rr},${rr} v${h - rr} h-${w} z`;
}

export function KpiTile({ label, value, hint, tone = "" }) {
  return (
    <div className={`tile ${tone}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {hint && <div className="hint">{hint}</div>}
    </div>
  );
}

export function Legend({ items }) {
  return (
    <div className="legend">
      {items.map(([label, color]) => (
        <span className="key" key={label}>
          <span className="swatch" style={{ background: color }} /> {label}
        </span>
      ))}
    </div>
  );
}

/** Horizontal bars with category labels left and direct value labels right. */
export function HBars({ rows, color = "var(--series-1)", unit = "", labelWidth = 170, format = fmt, maxWidth = 640 }) {
  const barH = 16, gap = 10, width = 560;
  const max = Math.max(...rows.map((r) => r.value)) || 1;
  const plotW = width - labelWidth - 56;
  const height = rows.length * (barH + gap);
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(maxWidth)}>
      {rows.map((r, i) => {
        const y = i * (barH + gap);
        const w = Math.max((r.value / max) * plotW, 2);
        return (
          <g key={r.label} className="hover-target">
            <title>{`${r.label}: ${format(r.value)}${unit}`}</title>
            <text x={labelWidth - 8} y={y + barH - 3} textAnchor="end">{r.label}</text>
            <path d={barPath(labelWidth, y, w, barH, 4, true)} fill={r.color || color} />
            <text className="val" x={labelWidth + w + 6} y={y + barH - 3}>{format(r.value)}{unit}</text>
          </g>
        );
      })}
    </svg>
  );
}

/** Vertical bars (one series), e.g. course status or histogram. */
export function VBars({ rows, color = "var(--series-1)", height = 190 }) {
  const width = 560, pad = 8, base = height - 24;
  const bw = Math.min(54, (width - 2 * pad) / rows.length - 10);
  const max = Math.max(...rows.map((r) => r.value)) || 1;
  const step = (width - 2 * pad) / rows.length;
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(600)}>
      <line className="gridline" x1={pad} x2={width - pad} y1={base} y2={base} />
      {rows.map((r, i) => {
        const h = Math.max((r.value / max) * (base - 26), 2);
        const x = pad + i * step + (step - bw) / 2;
        return (
          <g key={r.label} className="hover-target">
            <title>{`${r.label}: ${fmt(r.value)}`}</title>
            <path d={barPath(x, base - h, bw, h, 4, false)} fill={r.color || color} />
            <text className="val" x={x + bw / 2} y={base - h - 6} textAnchor="middle">{fmt(r.value)}</text>
            <text className="tick" x={x + bw / 2} y={base + 15} textAnchor="middle">{r.label}</text>
          </g>
        );
      })}
    </svg>
  );
}

/** Grouped vertical bars for exactly two series (e.g. previous vs latest). */
export function GroupedVBars({ rows, seriesA, seriesB, colorA = "var(--series-1-soft)", colorB = "var(--series-1)" }) {
  const width = 560, height = 210, pad = 8, base = height - 24, bw = 22;
  const max = Math.max(...rows.flatMap((r) => [r.a, r.b])) || 1;
  const step = (width - 2 * pad) / rows.length;
  return (
    <>
      <Legend items={[[seriesA, colorA], [seriesB, colorB]]} />
      <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(600)}>
        <line className="gridline" x1={pad} x2={width - pad} y1={base} y2={base} />
        {rows.map((r, i) => {
          const x = pad + i * step + step / 2;
          const hA = (r.a / max) * (base - 30), hB = (r.b / max) * (base - 30);
          return (
            <g key={r.label} className="hover-target">
              <title>{`${r.label} - ${seriesA}: ${fmt(r.a)}, ${seriesB}: ${fmt(r.b)}`}</title>
              <path d={barPath(x - bw - 1, base - hA, bw, Math.max(hA, 1), 4, false)} fill={colorA} />
              <path d={barPath(x + 1, base - hB, bw, Math.max(hB, 1), 4, false)} fill={colorB} />
              <text className="val" x={x + 1 + bw / 2} y={base - hB - 5} textAnchor="middle">{fmt(r.b)}</text>
              <text className="tick" x={x} y={base + 15} textAnchor="middle">{r.label}</text>
            </g>
          );
        })}
      </svg>
    </>
  );
}

/** 100% stacked horizontal bars; 2px surface gaps between segments. */
export function StackedPct({ rows, series, labelWidth = 120, showPctFrom = 8 }) {
  const width = 560, barH = 22, gap = 12;
  const height = rows.length * (barH + gap);
  const plotW = width - labelWidth - 8;
  return (
    <>
      <Legend items={series.map((s) => [s.label, s.color])} />
      <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(640)}>
        {rows.map((r, i) => {
          const y = i * (barH + gap);
          const total = series.reduce((t, s) => t + (r[s.key] || 0), 0) || 1;
          let x = labelWidth;
          return (
            <g key={r.label}>
              <text x={labelWidth - 8} y={y + barH - 6} textAnchor="end">{r.label}</text>
              {series.map((s) => {
                const pct = ((r[s.key] || 0) / total) * 100;
                const w = (pct / 100) * plotW;
                const seg = (
                  <g key={s.key} className="hover-target">
                    <title>{`${r.label} - ${s.label}: ${pct.toFixed(0)}%`}</title>
                    <rect x={x} y={y} width={Math.max(w, 0)} height={barH} fill={s.color}
                      stroke="var(--surface-1)" strokeWidth="2" rx="3" />
                    {pct >= showPctFrom && (
                      <text x={x + w / 2} y={y + barH - 6} textAnchor="middle"
                        fill={s.dark ? "#fff" : "var(--ink)"} fontWeight="600">{pct.toFixed(0)}%</text>
                    )}
                  </g>
                );
                x += w;
                return seg;
              })}
            </g>
          );
        })}
      </svg>
    </>
  );
}

/** Grid heatmap (departments x categories, risk matrix, transitions, 9-box). */
export function HeatGrid({ cols, rowsLabels, values, format = (v) => v, cellH = 34, labelWidth = 110, tooltip, cellText, maxWidth = 680 }) {
  const width = 640;
  const cellW = (width - labelWidth) / cols.length;
  const height = rowsLabels.length * cellH + 46;
  const flat = values.flat();
  const vMin = Math.min(...flat), vMax = Math.max(...flat) || 1;
  const steps = ["var(--seq-1)", "var(--seq-2)", "var(--seq-3)", "var(--seq-4)", "var(--seq-5)", "var(--seq-6)", "var(--seq-7)"];
  const colorOf = (v) => steps[Math.min(6, Math.floor(((v - vMin) / (vMax - vMin || 1)) * 6.999))];
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(maxWidth)}>
      {cols.map((c, j) => (
        <text key={c} className="tick" x={labelWidth + j * cellW + cellW / 2} y={14} textAnchor="middle">{c}</text>
      ))}
      {rowsLabels.map((rl, i) => (
        <g key={rl}>
          <text x={labelWidth - 8} y={24 + i * cellH + cellH / 2 + 4} textAnchor="end">{rl}</text>
          {cols.map((c, j) => {
            const v = values[i][j];
            const strong = (v - vMin) / (vMax - vMin || 1) > 0.55;
            return (
              <g key={c} className="hover-target">
                <title>{tooltip ? tooltip(rl, c, v) : `${rl} x ${c}: ${format(v)}`}</title>
                <rect x={labelWidth + j * cellW} y={24 + i * cellH} width={cellW} height={cellH}
                  fill={colorOf(v)} stroke="var(--surface-1)" strokeWidth="2" rx="4" />
                <text x={labelWidth + j * cellW + cellW / 2} y={24 + i * cellH + cellH / 2 + 4}
                  textAnchor="middle" fontWeight="600"
                  fill={strong ? "#fff" : "var(--ink)"}>{cellText ? cellText(rl, c, v) : format(v)}</text>
              </g>
            );
          })}
        </g>
      ))}
    </svg>
  );
}

/** Candidate funnel: left-aligned bars, count + share of the previous stage. */
export function Funnel({ stages }) {
  const width = 560, barH = 26, gap = 14, labelW = 150;
  const max = stages[0].count || 1;
  const height = stages.length * (barH + gap);
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(620)}>
      {stages.map((s, i) => {
        const w = Math.max((s.count / max) * (width - labelW - 120), 3);
        const y = i * (barH + gap);
        const pct = i ? ` (${Math.round((s.count / stages[i - 1].count) * 100)}% of previous)` : "";
        const color = i >= 3 ? "var(--series-3)" : i === 0 ? "var(--series-1-soft)" : "var(--series-1)";
        return (
          <g key={s.stage} className="hover-target">
            <title>{`${s.stage}: ${fmt(s.count)}${pct}`}</title>
            <text x={labelW - 8} y={y + barH - 8} textAnchor="end">{s.stage}</text>
            <path d={barPath(labelW, y, w, barH, 4, true)} fill={color} />
            <text className="val" x={labelW + w + 8} y={y + barH - 12}>{fmt(s.count)}</text>
            {i > 0 && <text className="tick" x={labelW + w + 8} y={y + barH + 1}>{pct.trim()}</text>}
          </g>
        );
      })}
    </svg>
  );
}

/** Two-series monthly trend with hover tooltips and direct end labels. */
export function TrendLine({ months, series }) {
  const width = 560, height = 200, padL = 36, padR = 110, padT = 12, base = height - 26;
  const max = Math.max(...series.flatMap((s) => s.values)) * 1.15 || 1;
  const x = (i) => padL + (i / Math.max(months.length - 1, 1)) * (width - padL - padR);
  const y = (v) => base - (v / max) * (base - padT);
  const ticks = months.map((m, i) => ({ m, i })).filter(({ m }) => m.endsWith("-01") || m.endsWith("-07"));
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(620)}>
      {[0.25, 0.5, 0.75, 1].map((f) => (
        <line key={f} className="gridline" x1={padL} x2={width - padR} y1={y(max * f)} y2={y(max * f)} />
      ))}
      <line className="gridline" x1={padL} x2={width - padR} y1={base} y2={base} />
      {ticks.map(({ m, i }) => (
        <text key={m} className="tick" x={x(i)} y={base + 16} textAnchor="middle">{m}</text>
      ))}
      {series.map((s) => (
        <g key={s.label}>
          <path d={s.values.map((v, i) => `${i ? "L" : "M"}${x(i)},${y(v)}`).join(" ")}
            fill="none" stroke={s.color} strokeWidth="2" strokeLinejoin="round" />
          <text x={x(months.length - 1) + 8} y={y(s.values[s.values.length - 1]) + 4}
            fill={s.color} fontWeight="700">{s.label}</text>
        </g>
      ))}
      {months.map((m, i) => (
        <g key={m} className="hover-target">
          <title>{`${m}: ${series.map((s) => `${s.label} ${s.values[i]}`).join(", ")}`}</title>
          <rect x={x(i) - 4} y={padT} width="8" height={base - padT} fill="transparent" />
          {series.map((s) => (
            <circle key={s.label} cx={x(i)} cy={y(s.values[i])} r="2.5" fill={s.color} />
          ))}
        </g>
      ))}
    </svg>
  );
}

/** Scatter: team rating vs outcome, radius = headcount, 2px surface ring. */
export function Scatter({ points, xLabel, yLabel }) {
  const width = 560, height = 280, padL = 44, padR = 16, padT = 10, padB = 36;
  const xs = points.map((p) => p.rating), ys = points.map((p) => p.outcome);
  const xMin = Math.min(...xs) - 0.1, xMax = Math.max(...xs) + 0.1;
  const yMin = Math.min(...ys) - 3, yMax = Math.max(...ys) + 3;
  const maxHead = Math.max(...points.map((p) => p.headcount));
  const x = (v) => padL + ((v - xMin) / (xMax - xMin)) * (width - padL - padR);
  const y = (v) => height - padB - ((v - yMin) / (yMax - yMin)) * (height - padT - padB);
  const r = (h) => 5 + Math.sqrt(h / maxHead) * 9;
  const xTicks = [3.0, 3.4, 3.8, 4.2].filter((t) => t > xMin && t < xMax);
  const yTicks = [50, 60, 70, 80, 90].filter((t) => t > yMin && t < yMax);
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img" style={svgStyle(620)}>
      {yTicks.map((t) => (
        <g key={t}>
          <line className="gridline" x1={padL} x2={width - padR} y1={y(t)} y2={y(t)} />
          <text className="tick" x={padL - 6} y={y(t) + 4} textAnchor="end">{t}</text>
        </g>
      ))}
      {xTicks.map((t) => (
        <text key={t} className="tick" x={x(t)} y={height - padB + 18} textAnchor="middle">{t.toFixed(1)}</text>
      ))}
      <text className="tick" x={(padL + width - padR) / 2} y={height - 4} textAnchor="middle">{xLabel}</text>
      <text className="tick" transform={`translate(12 ${(padT + height - padB) / 2}) rotate(-90)`} textAnchor="middle">{yLabel}</text>
      {points.map((p) => (
        <g key={p.team} className="hover-target">
          <title>{`${p.team} (${p.department}) - rating ${p.rating}, outcome ${p.outcome}, on-time ${p.on_time}%, ${p.headcount} people`}</title>
          <circle cx={x(p.rating)} cy={y(p.outcome)} r={r(p.headcount)}
            fill={p.on_time >= 50 ? "var(--series-1)" : "var(--series-1-soft)"}
            stroke="var(--surface-1)" strokeWidth="2" />
        </g>
      ))}
    </svg>
  );
}

/** Two thin bars per row: compare a highlighted group against the baseline. */
export function PairedHBars({ rows, labelA, labelB, colorA = "var(--crit)", colorB = "var(--series-1-soft)", unit = "%" }) {
  const width = 560, barH = 11, pairGap = 3, rowGap = 18, labelW = 185;
  const max = Math.max(...rows.flatMap((r) => [r.a, r.b])) * 1.25 || 1;
  const plotW = width - labelW - 52;
  const rowH = 2 * barH + pairGap + rowGap;
  return (
    <>
      <Legend items={[[labelA, colorA], [labelB, colorB]]} />
      <svg viewBox={`0 0 ${width} ${rows.length * rowH}`} role="img" style={svgStyle(640)}>
        {rows.map((r, i) => {
          const y = i * rowH;
          const wA = Math.max((r.a / max) * plotW, 2), wB = Math.max((r.b / max) * plotW, 2);
          return (
            <g key={r.label} className="hover-target">
              <title>{`${r.label} - ${labelA}: ${r.a}${unit}, ${labelB}: ${r.b}${unit}`}</title>
              <text x={labelW - 8} y={y + barH + 5} textAnchor="end">{r.label}</text>
              <path d={barPath(labelW, y, wA, barH, 3, true)} fill={colorA} />
              <text className="val" x={labelW + wA + 6} y={y + barH - 1} fontSize="11">{r.a}{unit}</text>
              <path d={barPath(labelW, y + barH + pairGap, wB, barH, 3, true)} fill={colorB} />
              <text className="val" x={labelW + wB + 6} y={y + 2 * barH + pairGap - 1} fontSize="11">{r.b}{unit}</text>
            </g>
          );
        })}
      </svg>
    </>
  );
}

export function DataTable({ columns, rows }) {
  return (
    <table>
      <thead>
        <tr>{columns.map((c) => <th key={c.key}>{c.label}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>{columns.map((c) => <td key={c.key}>{r[c.key]}</td>)}</tr>
        ))}
      </tbody>
    </table>
  );
}

export function Finding({ children }) {
  return <p className="finding"><b>Finding - </b>{children}</p>;
}

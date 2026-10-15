// Data access for the dashboard.
//
// Primary source: the insights-api Lambda, reached through CloudFront at
// /api/insights-api/<dataset> (VITE_API_URL is set by bin/deploy-frontend.sh).
// Fallback: the snapshot bundled at build time under /data/<dataset>.json, so
// the dashboard still renders if the Lambda cannot reach the data lake.

const API_BASE = `${import.meta.env.VITE_API_URL || ""}/api/insights-api`;

const DATASETS = [
  "meta", "summary", "performance", "skills", "promotion",
  "learning", "attrition", "teams", "coverage", "extras",
];

async function fetchJson(url) {
  const resp = await fetch(url, { headers: { Accept: "application/json" } });
  if (!resp.ok) throw new Error(`${url} -> HTTP ${resp.status}`);
  return resp.json();
}

export async function loadAll() {
  let source = "live";
  let data = {};
  try {
    const live = await Promise.all(DATASETS.map((n) => fetchJson(`${API_BASE}/${n}`)));
    DATASETS.forEach((n, i) => (data[n] = live[i]));
  } catch {
    source = "snapshot";
    const snap = await Promise.all(DATASETS.map((n) => fetchJson(`${import.meta.env.BASE_URL}data/${n}.json`)));
    DATASETS.forEach((n, i) => (data[n] = snap[i]));
  }
  return { source, data };
}

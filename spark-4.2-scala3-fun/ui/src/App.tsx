import { useEffect, useState } from "react";
import type { Results } from "./types";
import { flavor } from "./flavor";

const fmt = (n: number, d = 2) =>
  Number.isInteger(n) ? String(n) : n.toFixed(d);

function Bars({
  data,
  color,
  unit = "",
}: {
  data: { label: string; value: number }[];
  color: string;
  unit?: string;
}) {
  const max = Math.max(...data.map((d) => d.value), 0.0001);
  return (
    <div className="bars">
      {data.map((d) => (
        <div className="bar-row" key={d.label}>
          <span className="bar-label" title={d.label}>
            {d.label}
          </span>
          <div className="bar-track">
            <div
              className="bar-fill"
              style={{
                width: `${Math.max((d.value / max) * 100, 1.5)}%`,
                background: color,
              }}
            />
          </div>
          <span className="bar-value">
            {fmt(d.value)}
            {unit}
          </span>
        </div>
      ))}
    </div>
  );
}

function Card({
  tag,
  title,
  blurb,
  children,
}: {
  tag: string;
  title: string;
  blurb: string;
  children: React.ReactNode;
}) {
  return (
    <section className="card">
      <div className="card-head">
        <span className="tag">{tag}</span>
        <h2>{title}</h2>
        <p className="blurb">{blurb}</p>
      </div>
      {children}
    </section>
  );
}

export default function App() {
  const [data, setData] = useState<Results | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const r = await fetch(`/data/results.json?t=${Date.now()}`);
        const text = await r.text();
        if (!r.ok || !text.trimStart().startsWith("{")) {
          setErr("results.json not available yet");
          return;
        }
        setData(JSON.parse(text) as Results);
        setErr(null);
      } catch {
        setErr("results.json not available yet");
      }
    };
    load();
    const id = setInterval(load, 4000);
    return () => clearInterval(id);
  }, []);

  if (!data)
    return (
      <div className="empty">
        <div className="spinner" />
        <h1>Waiting for Spark results…</h1>
        <p>
          The job writes <code>out/results.json</code>. Run <code>./start.sh</code>{" "}
          (or <code>./test.sh</code>) first — this page refreshes automatically.
        </p>
        {err && <p className="dim">{err}</p>}
      </div>
    );

  const { runtime, metricView, vector, sketches, geo } = data;
  const gap = metricView.naiveSummedCustomers - metricView.governedTotalCustomers;

  return (
    <main style={{ ["--accent" as string]: flavor.accent }}>
      <header className="hero">
        <div className="hero-left">
          <h1>
            {flavor.title} <span className="dot">·</span>{" "}
            <em>{flavor.subtitle}</em>
          </h1>
          <p className="note">{flavor.note}</p>
        </div>
        <div className="chips">
          <span className="chip">Spark {runtime.sparkVersion}</span>
          <span className="chip">JDK {runtime.javaVersion}</span>
          {runtime.scalaCompiler && (
            <span className="chip">Scala {runtime.scalaCompiler}</span>
          )}
          {runtime.scalaRuntimeStdlib && (
            <span className="chip dimchip">
              stdlib {runtime.scalaRuntimeStdlib}
            </span>
          )}
          {runtime.pythonVersion && (
            <span className="chip">Python {runtime.pythonVersion}</span>
          )}
          {runtime.pyarrowVersion && (
            <span className="chip dimchip">PyArrow {runtime.pyarrowVersion}</span>
          )}
          <span className="chip">{runtime.builtinFunctions} functions</span>
        </div>
      </header>

      <div className="grid">
        <Card
          tag="new in 4.2"
          title="Metric views"
          blurb="A governed semantic layer inside Spark SQL. One YAML definition; the engine keeps the aggregation correct at every grain."
        >
          <div className="hero-stat">
            <div className="stat good">
              <span className="stat-num">{metricView.governedTotalCustomers}</span>
              <span className="stat-cap">MEASURE(customers)</span>
              <span className="stat-sub">governed · correct</span>
            </div>
            <div className="stat bad">
              <span className="stat-num">{metricView.naiveSummedCustomers}</span>
              <span className="stat-cap">SUM(per-region counts)</span>
              <span className="stat-sub">naive · double counts {gap}</span>
            </div>
          </div>
          <p className="explain">
            COUNT(DISTINCT customer) is <strong>not additive</strong>. Customers
            who buy in more than one region get counted twice when a dashboard
            re-aggregates a pre-grouped result. The metric view re-plans from the
            source, so the total stays{" "}
            <strong>{metricView.governedTotalCustomers}</strong>.
          </p>
          <Bars
            color={flavor.accent}
            data={metricView.byRegion.map((r) => ({
              label: `${r.region} · ${r.customers} cust`,
              value: r.revenue,
            }))}
          />
          <details>
            <summary>metric view YAML</summary>
            <pre>{metricView.yaml}</pre>
          </details>
        </Card>

        <Card
          tag="new in 4.2"
          title="Vector search · NEAREST BY"
          blurb={`Top-K ranking join over embeddings. Query: "${vector.queryText}" → [${vector.queryVector.join(", ")}]`}
        >
          <Bars
            color={flavor.accent2}
            data={vector.nearest.map((n) => ({
              label: n.name,
              value: n.similarity,
            }))}
          />
          <table>
            <thead>
              <tr>
                <th>product</th>
                <th>L2 distance</th>
                <th>cosine similarity</th>
              </tr>
            </thead>
            <tbody>
              {vector.nearest.map((n) => (
                <tr key={n.name}>
                  <td>{n.name}</td>
                  <td className="num">{n.distance.toFixed(4)}</td>
                  <td className="num">{n.similarity.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="explain">
            <code>
              JOIN products EXACT NEAREST 3 BY DISTANCE vector_l2_distance(qv,
              embedding)
            </code>{" "}
            — ranking is pushed into the join instead of a cross join plus
            window.
          </p>
        </Card>

        <Card
          tag="sketches"
          title="approx_top_k & theta sketch"
          blurb="Probabilistic aggregates for heavy hitters and cardinality, computed in one pass."
        >
          <Bars
            color={flavor.accent}
            data={sketches.topK.map((t) => ({ label: t.item, value: t.count }))}
            unit=" clicks"
          />
          <div className="pair">
            <div>
              <span className="pair-num">{sketches.exactDistinct}</span>
              <span className="pair-cap">COUNT(DISTINCT)</span>
            </div>
            <div>
              <span className="pair-num">{sketches.thetaSketchDistinct}</span>
              <span className="pair-cap">theta_sketch_estimate</span>
            </div>
          </div>
        </Card>

        <Card
          tag="new in 4.2"
          title="Native geospatial types"
          blurb="GEOMETRY and GEOGRAPHY are first-class types with SRID preservation — no external spatial extension."
        >
          <table>
            <thead>
              <tr>
                <th>site</th>
                <th>SRID</th>
                <th>type</th>
              </tr>
            </thead>
            <tbody>
              {geo.sample.map((g) => (
                <tr key={g.site}>
                  <td>{g.site}</td>
                  <td className="num">{g.srid}</td>
                  <td>
                    <code>{g.geometryType}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="explain">
            Shipping in 4.2 ({geo.availableStFunctions.length} ST functions):
          </p>
          <div className="pills">
            {geo.availableStFunctions.map((f) => (
              <span className="pill" key={f}>
                {f}
              </span>
            ))}
          </div>
        </Card>

        {data.arrowUdf && (
          <Card
            tag="new in 4.2"
            title="Arrow-optimized Python UDFs"
            blurb="Arrow is the default UDF path in 4.2 — the columnar transfer applies with no code change."
          >
            <div className="pair">
              <div>
                <span className="pair-num">
                  {data.arrowUdf.arrowEnabledByDefault}
                </span>
                <span className="pair-cap">
                  spark.sql.execution.pythonUDF.arrow.enabled
                </span>
              </div>
              <div>
                <span className="pair-num">{data.arrowUdf.rows}</span>
                <span className="pair-cap">rows through the UDF</span>
              </div>
            </div>
            <Bars
              color={flavor.accent2}
              data={data.arrowUdf.sample.map((s) => ({
                label: s.name,
                value: s.boosted,
              }))}
            />
          </Card>
        )}

        {data.dataSource && (
          <Card
            tag="python data source"
            title="Custom reader in pure Python"
            blurb="A DataSource registered once and read through the standard spark.read interface."
          >
            <table>
              <thead>
                <tr>
                  <th>id</th>
                  <th>label</th>
                </tr>
              </thead>
              <tbody>
                {data.dataSource.rows.map((r) => (
                  <tr key={r.id}>
                    <td className="num">{r.id}</td>
                    <td>{r.label}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>

      <footer>
        Data read live from <code>out/results.json</code> · refreshed every 4s ·{" "}
        {flavor.language}
      </footer>
    </main>
  );
}

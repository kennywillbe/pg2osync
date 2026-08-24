"use client";

import type { ClientSearchResult, SearchHit } from "@/lib/client-types";

type Props = {
  result: ClientSearchResult;
  query: string;
  setQuery: (q: string) => void;
  onSearch: () => void;
  autoRefresh: boolean;
  setAutoRefresh: (v: boolean) => void;
  onInspect: (id: string) => void;
};

// OpenSearch highlight fragments contain <em>…</em> markers. They are parsed
// here into React nodes instead of injected as HTML, so a crafted document
// value cannot become markup in the page.
function Highlighted({ text }: { text: string }) {
  const parts = text.split(/(<em>.*?<\/em>)/g);
  return (
    <>
      {parts.map((part, i) =>
        part.startsWith("<em>") ? (
          <mark key={i} className="hl">{part.slice(4, -5)}</mark>
        ) : (
          <span key={i}>{part}</span>
        ),
      )}
    </>
  );
}

export default function SearchPanel({
  result, query, setQuery, onSearch, autoRefresh, setAutoRefresh, onInspect,
}: Props) {
  return (
    <section className="panel">
      <h2>OpenSearch — demo_products index</h2>
      <p className="hint">Reads go here. This is what pg2osync produced.</p>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          onSearch();
        }}
        className="search-bar"
      >
        <input
          placeholder='search name, description, tags… try a typo ("keybaord")'
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button type="submit">Search</button>
        <button type="button" onClick={onSearch}>Refresh</button>
        <label className="switch">
          <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
          auto 2s
        </label>
        {result.indexExists && <span className="total">{result.total} docs</span>}
      </form>

      {result.indexExists === false && (
        <p className="notice">
          The <code>demo_products</code> index does not exist yet.
          pg2osync creates it on first sync — start it with{" "}
          <code>pg2osync run -c pg2osync.demo.toml</code>.
        </p>
      )}
      {result.indexExists === null && <p className="empty">Loading…</p>}

      <ul className="rows">
        {result.hits.map((h: SearchHit) => {
          const nameHl = h.highlight?.name?.[0];
          const descHl = h.highlight?.description?.[0];
          return (
            <li key={h.id} className="row hit">
              <div className="row-main">
                <span className="id">#{h.id}</span>
                <strong>
                  {nameHl ? <Highlighted text={nameHl} /> : String(h.source.name ?? "")}
                </strong>
                <span className="price">${String(h.source.price ?? "")}</span>
              </div>
              <div className="row-detail">
                {descHl ? <Highlighted text={descHl} /> : String(h.source.description ?? "")} —{" "}
                {Array.isArray(h.source.reviews)
                  ? `${h.source.reviews.length} review(s)`
                  : "no reviews"}
              </div>
              <div className="row-actions" style={{ marginTop: 6 }}>
                <button className="small secondary" onClick={() => onInspect(h.id)}>
                  Inspect document
                </button>
              </div>
            </li>
          );
        })}
        {result.indexExists && result.hits.length === 0 && (
          <li className="empty">No documents match.</li>
        )}
      </ul>
    </section>
  );
}

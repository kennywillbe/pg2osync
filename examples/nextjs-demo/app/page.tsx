"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import ProductPanel from "./components/ProductPanel";
import SearchPanel from "./components/SearchPanel";
import InspectorModal from "./components/InspectorModal";
import type { ClientSearchResult, HistoryEntry, Product, Review } from "@/lib/client-types";

const emptySearch: ClientSearchResult = { indexExists: null, total: 0, hits: [] };

export default function Home() {
  const [products, setProducts] = useState<Product[]>([]);
  const [reviews, setReviews] = useState<Review[]>([]);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);

  const [query, setQuery] = useState("");
  const [searchResult, setSearchResult] = useState<ClientSearchResult>(emptySearch);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [inspectingId, setInspectingId] = useState<string | null>(null);

  // The search panel refreshes itself on a timer so a change written on the
  // left shows up without manual clicks; auto mode is opt-in because polling
  // every 2s is noise when you are only typing.
  const queryRef = useRef(query);
  queryRef.current = query;

  const loadProducts = useCallback(async () => {
    const res = await fetch("/api/products");
    const data = await res.json();
    setProducts(data.products ?? []);
    setReviews(data.reviews ?? []);
    setSelected((prev) => {
      const live = new Set((data.products ?? []).map((p: Product) => p.id));
      const next = new Set([...prev].filter((id) => live.has(id)));
      return next.size === prev.size ? prev : next;
    });
  }, []);

  const runSearch = useCallback(async (q?: string) => {
    const term = q ?? queryRef.current;
    const res = await fetch(`/api/search?q=${encodeURIComponent(term)}`);
    setSearchResult(await res.json());
  }, []);

  useEffect(() => {
    loadProducts();
    runSearch("");
  }, [loadProducts, runSearch]);

  useEffect(() => {
    if (!autoRefresh) return;
    const timer = setInterval(() => runSearch(), 2000);
    return () => clearInterval(timer);
  }, [autoRefresh, runSearch]);

  // afterWrite is the single funnel for every mutation: record the measured
  // propagation, then reload both sides. Components hand in the fetch promise
  // so the timing banner and the reload stay consistent with what ran.
  const afterWrite = useCallback(
    async (
      label: string,
      request: Promise<Response>,
      extra?: (data: Record<string, unknown>) => string | undefined,
    ) => {
      setBusy(true);
      try {
        const res = await request;
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new Error(body.error ?? `HTTP ${res.status}`);
        }
        const data = await res.json();
        if (data.propagation) {
          setHistory((prev) =>
            [{ label, propagation: data.propagation, extra: extra?.(data), at: Date.now() }, ...prev].slice(0, 12),
          );
        }
        await loadProducts();
        await runSearch();
      } catch (err) {
        alert(`Request failed: ${err instanceof Error ? err.message : err}`);
      } finally {
        setBusy(false);
      }
    },
    [loadProducts, runSearch],
  );

  const landedTimes = history.filter((h) => h.propagation.landed).map((h) => h.propagation.ms);
  const stats = landedTimes.length > 0
    ? {
        min: Math.min(...landedTimes),
        avg: Math.round(landedTimes.reduce((a, b) => a + b, 0) / landedTimes.length),
        max: Math.max(...landedTimes),
      }
    : null;
  const last = history[0];

  function toggleSelect(id: number) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleExpand(id: number) {
    setExpandedId((prev) => (prev === id ? null : id));
  }

  return (
    <main>
      <header className="banner">
        <h1>pg2osync demo</h1>
        <p>
          The left panel writes only to <strong>PostgreSQL</strong>. The right
          panel reads only from <strong>OpenSearch</strong>. Nothing in this
          app ever writes to OpenSearch directly — every document you see on
          the right arrived because a separate <code>pg2osync</code> process
          tailed the PostgreSQL replication log and indexed it. That gap is
          the propagation time shown below.
        </p>

        {last && (
          <div className={`propagation ${last.propagation.landed ? "ok" : "timeout"}`}>
            {last.label}:{" "}
            {last.propagation.landed ? (
              <strong>{last.propagation.ms} ms</strong>
            ) : (
              <strong>not visible after {last.propagation.ms} ms — is pg2osync running?</strong>
            )}
            {last.extra ? ` (${last.extra})` : ""}
          </div>
        )}

        {stats && (
          <div className="history">
            <h3>Propagation history — min {stats.min} · avg {stats.avg} · max {stats.max} ms</h3>
            <ul>
              {history.map((h, i) => (
                <li key={h.at + "-" + i} className={h.propagation.landed ? "ok" : "timeout"} title={`${new Date(h.at).toLocaleTimeString()} — ${h.label}${h.extra ? ` (${h.extra})` : ""}`}>
                  {h.propagation.landed ? `${h.propagation.ms} ms` : "timeout"} · {h.label}
                </li>
              ))}
            </ul>
          </div>
        )}
      </header>

      <div className="columns">
        <ProductPanel
          products={products}
          reviews={reviews}
          busy={busy}
          selected={selected}
          expandedId={expandedId}
          onToggleSelect={toggleSelect}
          onToggleExpand={toggleExpand}
          afterWrite={(label, req, extra) => afterWrite(label, req, extra)}
        />
        <SearchPanel
          result={searchResult}
          query={query}
          setQuery={setQuery}
          onSearch={() => runSearch()}
          autoRefresh={autoRefresh}
          setAutoRefresh={setAutoRefresh}
          onInspect={setInspectingId}
        />
      </div>

      {inspectingId !== null && (
        <InspectorModal id={inspectingId} onClose={() => setInspectingId(null)} />
      )}
    </main>
  );
}

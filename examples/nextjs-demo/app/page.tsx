"use client";

import { useEffect, useState, useCallback } from "react";

type Product = {
  id: number;
  name: string;
  description: string;
  price: string;
  tags: string[];
  updated_at: string;
};

type Propagation = { landed: boolean; ms: number };

type LastAction = {
  label: string;
  propagation: Propagation;
};

type SearchHit = { id: string; source: Record<string, unknown> };

const emptyForm = { name: "", description: "", price: "", tags: "" };

function parseTags(input: string): string[] {
  return input
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

export default function Home() {
  const [products, setProducts] = useState<Product[]>([]);
  const [form, setForm] = useState(emptyForm);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editForm, setEditForm] = useState(emptyForm);
  const [busy, setBusy] = useState(false);
  const [lastAction, setLastAction] = useState<LastAction | null>(null);

  const [query, setQuery] = useState("");
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [indexExists, setIndexExists] = useState<boolean | null>(null);

  const loadProducts = useCallback(async () => {
    const res = await fetch("/api/products");
    const data = await res.json();
    setProducts(data.products ?? []);
  }, []);

  const runSearch = useCallback(async (q: string) => {
    const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    setIndexExists(data.indexExists);
    setHits(data.hits ?? []);
  }, []);

  useEffect(() => {
    loadProducts();
    runSearch("");
  }, [loadProducts, runSearch]);

  async function afterWrite(label: string, res: Response) {
    const data = await res.json();
    if (data.propagation) {
      setLastAction({ label, propagation: data.propagation });
    }
    await loadProducts();
    await runSearch(query);
    return data;
  }

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      const res = await fetch("/api/products", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: form.name,
          description: form.description,
          price: form.price || "0",
          tags: parseTags(form.tags),
        }),
      });
      await afterWrite(`Created "${form.name}"`, res);
      setForm(emptyForm);
    } finally {
      setBusy(false);
    }
  }

  function startEdit(p: Product) {
    setEditingId(p.id);
    setEditForm({
      name: p.name,
      description: p.description,
      price: p.price,
      tags: (p.tags ?? []).join(", "),
    });
  }

  async function handleUpdate(id: number) {
    setBusy(true);
    try {
      const res = await fetch(`/api/products/${id}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: editForm.name,
          description: editForm.description,
          price: editForm.price || "0",
          tags: parseTags(editForm.tags),
        }),
      });
      await afterWrite(`Updated "${editForm.name}"`, res);
      setEditingId(null);
    } finally {
      setBusy(false);
    }
  }

  async function handleDelete(p: Product) {
    setBusy(true);
    try {
      const res = await fetch(`/api/products/${p.id}`, { method: "DELETE" });
      await afterWrite(`Deleted "${p.name}"`, res);
    } finally {
      setBusy(false);
    }
  }

  async function handleTruncate() {
    if (!confirm("TRUNCATE demo_products? This removes every row at once, with no per-row DELETE events.")) {
      return;
    }
    setBusy(true);
    try {
      const res = await fetch("/api/products/truncate", { method: "POST" });
      await afterWrite("TRUNCATE demo_products", res);
    } finally {
      setBusy(false);
    }
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
        {lastAction && (
          <div className={`propagation ${lastAction.propagation.landed ? "ok" : "timeout"}`}>
            {lastAction.label}:{" "}
            {lastAction.propagation.landed ? (
              <strong>{lastAction.propagation.ms} ms</strong>
            ) : (
              <strong>not visible after {lastAction.propagation.ms} ms — is pg2osync running?</strong>
            )}
          </div>
        )}
      </header>

      <div className="columns">
        <section className="panel">
          <h2>PostgreSQL — demo_products</h2>
          <p className="hint">Writes go here. This table is the source of truth.</p>

          <form onSubmit={handleCreate} className="form">
            <input
              placeholder="name"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              required
            />
            <input
              placeholder="description"
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
            />
            <input
              placeholder="price"
              value={form.price}
              onChange={(e) => setForm({ ...form, price: e.target.value })}
              inputMode="decimal"
            />
            <input
              placeholder="tags (comma separated)"
              value={form.tags}
              onChange={(e) => setForm({ ...form, tags: e.target.value })}
            />
            <button type="submit" disabled={busy}>
              Create
            </button>
          </form>

          <button className="danger" onClick={handleTruncate} disabled={busy}>
            TRUNCATE table
          </button>

          <ul className="rows">
            {products.map((p) => (
              <li key={p.id} className="row">
                {editingId === p.id ? (
                  <div className="edit-form">
                    <input
                      value={editForm.name}
                      onChange={(e) => setEditForm({ ...editForm, name: e.target.value })}
                    />
                    <input
                      value={editForm.description}
                      onChange={(e) => setEditForm({ ...editForm, description: e.target.value })}
                    />
                    <input
                      value={editForm.price}
                      onChange={(e) => setEditForm({ ...editForm, price: e.target.value })}
                      inputMode="decimal"
                    />
                    <input
                      value={editForm.tags}
                      onChange={(e) => setEditForm({ ...editForm, tags: e.target.value })}
                    />
                    <div className="row-actions">
                      <button onClick={() => handleUpdate(p.id)} disabled={busy}>
                        Save
                      </button>
                      <button onClick={() => setEditingId(null)}>Cancel</button>
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="row-main">
                      <span className="id">#{p.id}</span>
                      <strong>{p.name}</strong>
                      <span className="price">${p.price}</span>
                    </div>
                    <div className="row-detail">
                      {p.description} — {(p.tags ?? []).join(", ") || "no tags"}
                    </div>
                    <div className="row-actions">
                      <button onClick={() => startEdit(p)}>Edit</button>
                      <button className="danger" onClick={() => handleDelete(p)} disabled={busy}>
                        Delete
                      </button>
                    </div>
                  </>
                )}
              </li>
            ))}
            {products.length === 0 && <li className="empty">No rows yet.</li>}
          </ul>
        </section>

        <section className="panel">
          <h2>OpenSearch — demo_products index</h2>
          <p className="hint">Reads go here. This is what pg2osync produced.</p>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              runSearch(query);
            }}
            className="form"
          >
            <input
              placeholder="search name, description, tags…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <button type="submit">Search</button>
            <button type="button" onClick={() => runSearch(query)}>
              Refresh
            </button>
          </form>

          {indexExists === false && (
            <p className="notice">
              The <code>demo_products</code> index does not exist yet.
              pg2osync creates it on first sync — start it with{" "}
              <code>pg2osync run -c pg2osync.demo.toml</code>.
            </p>
          )}

          <ul className="rows">
            {hits.map((h) => (
              <li key={h.id} className="row">
                <div className="row-main">
                  <span className="id">#{h.id}</span>
                  <strong>{String(h.source.name ?? "")}</strong>
                  <span className="price">${String(h.source.price ?? "")}</span>
                </div>
                <div className="row-detail">
                  {String(h.source.description ?? "")} —{" "}
                  {Array.isArray(h.source.tags) ? h.source.tags.join(", ") : "no tags"}
                </div>
              </li>
            ))}
            {indexExists && hits.length === 0 && <li className="empty">No documents match.</li>}
          </ul>
        </section>
      </div>
    </main>
  );
}

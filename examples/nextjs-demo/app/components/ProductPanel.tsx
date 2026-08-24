"use client";

import { useState } from "react";
import type { Product, Review } from "@/lib/client-types";

type Props = {
  products: Product[];
  reviews: Review[];
  busy: boolean;
  selected: Set<number>;
  expandedId: number | null;
  onToggleSelect: (id: number) => void;
  onToggleExpand: (id: number) => void;
  afterWrite: (label: string, res: Promise<Response>, extra?: (data: Record<string, unknown>) => string | undefined) => Promise<void>;
};

const emptyForm = { name: "", description: "", price: "", tags: "", internalNote: "", supplierEmail: "" };

function parseTags(input: string): string[] {
  return input
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

export default function ProductPanel({
  products, reviews, busy, selected, expandedId,
  onToggleSelect, onToggleExpand, afterWrite,
}: Props) {
  const [form, setForm] = useState(emptyForm);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editForm, setEditForm] = useState(emptyForm);
  const [bulkCount, setBulkCount] = useState("10");
  const [pricePercent, setPricePercent] = useState("10");
  const [deleteOldest, setDeleteOldest] = useState("5");

  // Review forms are kept per product id so several rows can be open at once
  // without clobbering each other's draft.
  const [reviewDrafts, setReviewDrafts] = useState<Record<number, { author: string; rating: string; comment: string }>>({});

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    await afterWrite(`Created "${form.name}"`, fetch("/api/products", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: form.name,
        description: form.description,
        price: form.price || "0",
        tags: parseTags(form.tags),
        internalNote: form.internalNote,
        supplierEmail: form.supplierEmail,
      }),
    }));
    setForm(emptyForm);
  }

  async function handleUpdate(id: number) {
    await afterWrite(`Updated #${id}`, fetch(`/api/products/${id}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: editForm.name,
        description: editForm.description,
        price: editForm.price || "0",
        tags: parseTags(editForm.tags),
      }),
    }));
    setEditingId(null);
  }

  function startEdit(p: Product) {
    setEditingId(p.id);
    setEditForm({
      ...emptyForm,
      name: p.name,
      description: p.description,
      price: p.price,
      tags: (p.tags ?? []).join(", "),
    });
  }

  async function bulkCreate() {
    await afterWrite(
      `Bulk create ×${bulkCount}`,
      fetch("/api/products/bulk", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ count: Number(bulkCount) }),
      }),
      (d) => d.count ? `ids ${d.firstId}–${d.lastId}` : undefined,
    );
  }

  async function bulkPrice(direction: 1 | -1) {
    const percent = direction * Math.abs(Number(pricePercent));
    await afterWrite(
      `Prices ${percent > 0 ? "+" : ""}${percent}%`,
      fetch("/api/products/bulk", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ percent }),
      }),
      (d) => d.updated ? `${d.updated} rows` : undefined,
    );
  }

  async function deleteSelected() {
    if (selected.size === 0 || !confirm(`Delete ${selected.size} selected row(s)?`)) return;
    const ids = [...selected];
    await afterWrite(
      `Delete ${ids.length} selected`,
      fetch("/api/products/bulk", {
        method: "DELETE",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ids }),
      }),
    );
  }

  async function deleteOldestRows() {
    await afterWrite(
      `Delete oldest ×${deleteOldest}`,
      fetch("/api/products/bulk", {
        method: "DELETE",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ oldest: Number(deleteOldest) }),
      }),
    );
  }

  async function mixedTransaction() {
    await afterWrite(
      "Mixed transaction (insert+update+delete)",
      fetch("/api/products/transaction", { method: "POST" }),
    );
  }

  async function truncate() {
    if (!confirm("TRUNCATE demo_products? This removes every row at once, with no per-row DELETE events.")) return;
    await afterWrite(
      "TRUNCATE demo_products",
      fetch("/api/products/truncate", { method: "POST" }),
    );
  }

  async function addReview(productId: number) {
    const draft = reviewDrafts[productId] ?? { author: "", rating: "5", comment: "" };
    if (!draft.author.trim()) return;
    await afterWrite(
      `Review on #${productId}`,
      fetch("/api/reviews", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          productId,
          author: draft.author,
          rating: Number(draft.rating),
          comment: draft.comment,
        }),
      }),
    );
    setReviewDrafts({ ...reviewDrafts, [productId]: { author: "", rating: "5", comment: "" } });
  }

  async function deleteReview(reviewId: number) {
    await afterWrite(
      `Delete review #${reviewId}`,
      fetch(`/api/reviews/${reviewId}`, { method: "DELETE" }),
    );
  }

  function setDraft(productId: number, patch: Partial<{ author: string; rating: string; comment: string }>) {
    const current = reviewDrafts[productId] ?? { author: "", rating: "5", comment: "" };
    setReviewDrafts({ ...reviewDrafts, [productId]: { ...current, ...patch } });
  }

  return (
    <section className="panel">
      <h2>PostgreSQL — demo_products</h2>
      <p className="hint">
        Writes go here. This table is the source of truth; OpenSearch is never
        written directly.
      </p>

      <form onSubmit={handleCreate} className="form">
        <input placeholder="name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
        <input placeholder="description" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
        <input placeholder="price" value={form.price} onChange={(e) => setForm({ ...form, price: e.target.value })} inputMode="decimal" />
        <input placeholder="tags (comma separated)" value={form.tags} onChange={(e) => setForm({ ...form, tags: e.target.value })} />
        <input placeholder="internal note (excluded from index)" value={form.internalNote} onChange={(e) => setForm({ ...form, internalNote: e.target.value })} />
        <input placeholder="supplier email (redacted in index)" value={form.supplierEmail} onChange={(e) => setForm({ ...form, supplierEmail: e.target.value })} />
        <button type="submit" disabled={busy}>Create</button>
      </form>

      <div className="toolbar">
        <span className="label">Bulk:</span>
        <input type="number" min={1} max={5000} value={bulkCount} onChange={(e) => setBulkCount(e.target.value)} />
        <button onClick={bulkCreate} disabled={busy}>Create N random</button>
        <button className="secondary" onClick={() => bulkPrice(1)} disabled={busy}>+{pricePercent || 10}% price</button>
        <button className="secondary" onClick={() => bulkPrice(-1)} disabled={busy}>−{pricePercent || 10}% price</button>
        <input type="number" value={pricePercent} onChange={(e) => setPricePercent(e.target.value)} title="price change %" />
        <span className="label">|</span>
        <input type="number" min={1} value={deleteOldest} onChange={(e) => setDeleteOldest(e.target.value)} />
        <button className="danger" onClick={deleteOldestRows} disabled={busy}>Delete oldest N</button>
        <button className="danger" onClick={deleteSelected} disabled={busy || selected.size === 0}>
          Delete selected ({selected.size})
        </button>
      </div>

      <div className="toolbar">
        <button className="secondary" onClick={mixedTransaction} disabled={busy}>
          Mixed transaction demo
        </button>
        <button className="danger" onClick={truncate} disabled={busy}>TRUNCATE table</button>
      </div>

      <ul className="rows">
        {products.map((p) => {
          const productReviews = reviews.filter((r) => r.product_id === p.id);
          const isExpanded = expandedId === p.id;
          return (
            <li key={p.id} className={`row ${selected.has(p.id) ? "selected" : ""}`}>
              {editingId === p.id ? (
                <div className="edit-form">
                  <input value={editForm.name} onChange={(e) => setEditForm({ ...editForm, name: e.target.value })} />
                  <input value={editForm.description} onChange={(e) => setEditForm({ ...editForm, description: e.target.value })} />
                  <input value={editForm.price} onChange={(e) => setEditForm({ ...editForm, price: e.target.value })} inputMode="decimal" />
                  <input value={editForm.tags} onChange={(e) => setEditForm({ ...editForm, tags: e.target.value })} />
                  <div className="row-actions">
                    <button onClick={() => handleUpdate(p.id)} disabled={busy}>Save</button>
                    <button onClick={() => setEditingId(null)}>Cancel</button>
                  </div>
                </div>
              ) : (
                <>
                  <div className="row-main">
                    <input
                      type="checkbox"
                      checked={selected.has(p.id)}
                      onChange={() => onToggleSelect(p.id)}
                      aria-label={`select ${p.name}`}
                    />
                    <span className="id">#{p.id}</span>
                    <strong>{p.name}</strong>
                    <span className="price">${p.price}</span>
                  </div>
                  <div className="row-detail">
                    {p.description} — {(p.tags ?? []).join(", ") || "no tags"}
                  </div>
                  <div className="row-actions">
                    <button className="small" onClick={() => onToggleExpand(p.id)}>
                      {isExpanded ? "Hide" : `Reviews (${productReviews.length})`}
                    </button>
                    <button className="small" onClick={() => startEdit(p)}>Edit</button>
                    <button className="small danger" onClick={() => afterWrite(`Deleted "${p.name}"`, fetch(`/api/products/${p.id}`, { method: "DELETE" }))} disabled={busy}>
                      Delete
                    </button>
                  </div>
                  {isExpanded && (
                    <div className="reviews">
                      <h4>demo_reviews — embedded as &quot;reviews&quot; in OpenSearch</h4>
                      {productReviews.map((r) => (
                        <div key={r.id} className="review">
                          <span className="stars">{"★".repeat(r.rating)}{"☆".repeat(5 - r.rating)}</span>
                          <strong>{r.author}</strong>
                          <span className="comment">{r.comment}</span>
                          <button className="small danger" onClick={() => deleteReview(r.id)} disabled={busy}>×</button>
                        </div>
                      ))}
                      {productReviews.length === 0 && <div className="empty">No reviews yet.</div>}
                      <div className="review-form">
                        <select value={reviewDrafts[p.id]?.rating ?? "5"} onChange={(e) => setDraft(p.id, { rating: e.target.value })}>
                          {[5, 4, 3, 2, 1].map((n) => <option key={n} value={n}>{n}★</option>)}
                        </select>
                        <input name="author" placeholder="author" value={reviewDrafts[p.id]?.author ?? ""} onChange={(e) => setDraft(p.id, { author: e.target.value })} />
                        <input name="comment" placeholder="comment" value={reviewDrafts[p.id]?.comment ?? ""} onChange={(e) => setDraft(p.id, { comment: e.target.value })} />
                        <button className="small" onClick={() => addReview(p.id)} disabled={busy || !(reviewDrafts[p.id]?.author ?? "").trim()}>
                          Add review
                        </button>
                      </div>
                    </div>
                  )}
                </>
              )}
            </li>
          );
        })}
        {products.length === 0 && <li className="empty">No rows yet.</li>}
      </ul>
    </section>
  );
}

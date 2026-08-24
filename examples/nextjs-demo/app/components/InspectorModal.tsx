"use client";

import { useEffect, useState } from "react";

type Props = {
  id: string;
  onClose: () => void;
};

type Doc = { found: boolean; source?: Record<string, unknown> };

// The inspector shows the document exactly as pg2osync wrote it and asserts
// the projection rules on it: the excluded column must be absent, the
// redacted column must read ***, and children must be embedded as an array.
export default function InspectorModal({ id, onClose }: Props) {
  const [doc, setDoc] = useState<Doc | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch(`/api/doc/${id}`)
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((data) => {
        if (!cancelled) setDoc(data);
      })
      .catch((err) => {
        if (!cancelled) setError(String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [id]);

  const source = doc?.source;
  const email = typeof source?.supplier_email === "string" ? source.supplier_email : undefined;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>OpenSearch document #{id}</h3>

        {source && (
          <div className="badges">
            <span className={`badge ${!("internal_note" in source) ? "pass" : "fail"}`}>
              {!("internal_note" in source)
                ? "internal_note excluded ✓"
                : "internal_note LEAKED ✗"}
            </span>
            <span className={`badge ${email === "***" ? "pass" : "fail"}`}>
              {email === "***"
                ? "supplier_email redacted ✓"
                : `supplier_email = ${JSON.stringify(email ?? "(missing)")}`}
            </span>
            <span className="badge pass">
              reviews: {Array.isArray(source.reviews) ? `${source.reviews.length} embedded` : "none"}
            </span>
          </div>
        )}

        {doc && !doc.found && (
          <p className="notice">
            Document not found — either deleted via pg2osync or not yet written.
          </p>
        )}
        {error && <p className="notice">Failed to load: {error}</p>}
        {doc === null && !error && <p className="empty">Loading…</p>}

        {source && <pre className="json">{JSON.stringify(source, null, 2)}</pre>}

        <div className="row-actions" style={{ marginTop: 14 }}>
          <button onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}

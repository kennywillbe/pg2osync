const PG2OSYNC_API = process.env.PG2OSYNC_API_URL ?? "http://127.0.0.1:9131";

/**
 * Waits until everything committed before now is searchable, by asking
 * pg2osync rather than polling OpenSearch.
 *
 * The endpoint reads the source position itself and returns only once the
 * pipeline has written past it and refreshed the index, so a query made right
 * after this resolves is guaranteed to see the write. Polling used to be the
 * only option here, and it needed a different check for every operation —
 * present for a create, changed for an update, gone for a delete.
 */
export async function waitUntilSearchable(
  { timeoutMs = 8_000 } = {},
): Promise<{ landed: boolean; ms: number }> {
  const start = performance.now();
  const url = `${PG2OSYNC_API}/synced?refresh=true&timeout=${timeoutMs}`;
  try {
    const res = await fetch(url);
    const body = await res.json();
    return { landed: res.ok && body.synced === true, ms: Math.round(performance.now() - start) };
  } catch {
    // the endpoint being unreachable must not fail the write that already
    // succeeded; the caller shows a stale list rather than an error
    return { landed: false, ms: Math.round(performance.now() - start) };
  }
}

import { getProductDoc } from "./opensearch";

/**
 * Polls OpenSearch after a PostgreSQL write until the effect of that write is
 * visible, and reports how long it took. This is the whole point of the demo:
 * the number on screen is a measurement, not a claim.
 */
export async function measurePropagation(
  check: () => Promise<boolean>,
  { timeoutMs = 15_000, intervalMs = 100 } = {},
): Promise<{ landed: boolean; ms: number }> {
  const start = performance.now();
  const deadline = start + timeoutMs;

  while (true) {
    if (await check()) {
      return { landed: true, ms: Math.round(performance.now() - start) };
    }
    if (performance.now() >= deadline) {
      return { landed: false, ms: Math.round(performance.now() - start) };
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/**
 * Variant for writes affecting many rows at once. Checking every document
 * would take one request per row per poll, so only a sample is verified —
 * enough to know pg2osync processed the batch, cheap enough to stay fast.
 * The count check catches truncation-style changes where sampling could
 * pass by accident.
 */
export async function measureBulkPropagation(
  ids: Array<number | string>,
  docSatisfies: (id: number | string, doc: Record<string, unknown> | null) => boolean,
  { timeoutMs = 30_000, intervalMs = 200, sampleSize = 5 } = {},
): Promise<{ landed: boolean; ms: number; checked: number }> {
  // Deterministic spread across the batch rather than pure random: with a
  // sorted sample you see both ends of the id range land.
  const unique = [...new Set(ids.map(String))].sort();
  const step = Math.max(1, Math.floor(unique.length / sampleSize));
  const sampled = unique.filter((_, i) => i % step === 0).slice(0, sampleSize);

  const result = await measurePropagation(async () => {
    for (const id of sampled) {
      if (!docSatisfies(id, await getProductDoc(id))) return false;
    }
    return true;
  }, { timeoutMs, intervalMs });

  return { ...result, checked: sampled.length };
}

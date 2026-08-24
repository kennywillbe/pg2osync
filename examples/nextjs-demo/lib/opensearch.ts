// The demo only ever reads from OpenSearch, with plain `fetch`. It never
// issues an index/update/delete request here — those documents exist only
// because pg2osync wrote them after tailing the PostgreSQL WAL.
const BASE_URL = process.env.DEMO_OPENSEARCH_URL ?? "http://localhost:9200";
export const DEMO_INDEX = process.env.DEMO_OS_INDEX ?? "demo_products";

export type SearchResult = {
  indexExists: boolean;
  total: number;
  hits: Array<{ id: string; source: Record<string, unknown> }>;
};

async function indexExists(index: string): Promise<boolean> {
  const res = await fetch(`${BASE_URL}/${index}`, { method: "HEAD" });
  return res.ok;
}

/**
 * Queries the index pg2osync maintains. Returns `indexExists: false` instead
 * of throwing when the index is missing, so the UI can say "pg2osync hasn't
 * created this index yet" rather than surfacing a raw fetch error — the app
 * must work even before pg2osync has ever run.
 */
export async function searchProducts(query: string): Promise<SearchResult> {
  if (!(await indexExists(DEMO_INDEX))) {
    return { indexExists: false, total: 0, hits: [] };
  }

  const body = query.trim()
    ? {
        query: {
          multi_match: {
            query,
            fields: ["name", "description", "tags"],
            fuzziness: "AUTO",
          },
        },
      }
    : { query: { match_all: {} } };

  const res = await fetch(`${BASE_URL}/${DEMO_INDEX}/_search?size=50`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    // A brand-new index has no mapping until its first document lands, and
    // sorting on an unmapped field is an error rather than an empty result.
    // (A TRUNCATE is safe here: it clears documents and leaves the mapping.)
    body: JSON.stringify({ ...body, sort: [{ id: { order: "asc", unmapped_type: "long" } }] }),
  });

  if (!res.ok) {
    throw new Error(`OpenSearch search failed: ${res.status} ${await res.text()}`);
  }

  const json = await res.json();
  return {
    indexExists: true,
    total: json.hits?.total?.value ?? 0,
    hits: (json.hits?.hits ?? []).map((h: { _id: string; _source: Record<string, unknown> }) => ({
      id: h._id,
      source: h._source,
    })),
  };
}

/** Looks up a single document by id, for polling "has this row landed yet?". */
export async function getProductDoc(
  id: number | string,
): Promise<Record<string, unknown> | null> {
  if (!(await indexExists(DEMO_INDEX))) return null;

  const res = await fetch(`${BASE_URL}/${DEMO_INDEX}/_doc/${id}`);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(`OpenSearch get failed: ${res.status} ${await res.text()}`);
  }
  const json = await res.json();
  return json._source ?? null;
}

/** Total document count, used to detect a TRUNCATE landing (count drops to 0). */
export async function countProducts(): Promise<number | null> {
  if (!(await indexExists(DEMO_INDEX))) return null;
  const res = await fetch(`${BASE_URL}/${DEMO_INDEX}/_count`);
  if (!res.ok) return null;
  const json = await res.json();
  return json.count ?? 0;
}

/**
 * OpenSearch's default `refresh_interval` (1s) means a document can be
 * fetched by id (real-time get, from the translog) well before `_search` can
 * find it (which needs a segment refresh). Without this, the propagation
 * number the API reports — measured with `getProductDoc`, the honest "has
 * pg2osync written it yet" check — could read faster than the search panel
 * actually updates, which would undercut the whole point of the demo. This
 * is a read-only visibility operation: it makes existing data searchable,
 * it does not write or change any document.
 */
export async function refreshIndex(): Promise<void> {
  if (!(await indexExists(DEMO_INDEX))) return;
  await fetch(`${BASE_URL}/${DEMO_INDEX}/_refresh`, { method: "POST" });
}

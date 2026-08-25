import { NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { countProducts, refreshIndex } from "@/lib/opensearch";
import { waitUntilSearchable } from "@/lib/propagation";

// TRUNCATE is the path worth calling out separately: it never appears as a
// row-level DELETE in the WAL, so a naive CDC pipeline built only around
// insert/update/delete would miss it entirely. pg2osync decodes the
// TRUNCATE event itself and clears the target index to match.
export async function POST() {
  const before = await countProducts();
  // demo_references references demo_products, so PostgreSQL refuses to
  // truncate the parent alone; both go in one statement, one WAL record.
  await pool.query("TRUNCATE TABLE demo_products, demo_reviews RESTART IDENTITY");

  const propagation = await waitUntilSearchable();

  return NextResponse.json({ ok: true, before, propagation });
}

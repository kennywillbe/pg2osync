import { NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { countProducts, refreshIndex } from "@/lib/opensearch";
import { measurePropagation } from "@/lib/propagation";

// TRUNCATE is the path worth calling out separately: it never appears as a
// row-level DELETE in the WAL, so a naive CDC pipeline built only around
// insert/update/delete would miss it entirely. pg2osync decodes the
// TRUNCATE event itself and clears the target index to match.
export async function POST() {
  const before = await countProducts();
  await pool.query("TRUNCATE TABLE demo_products RESTART IDENTITY");

  const propagation = await measurePropagation(async () => {
    const count = await countProducts();
    return count === 0 || count === null; // null: index doesn't exist, so nothing to see either
  });
  await refreshIndex();

  return NextResponse.json({ ok: true, before, propagation });
}

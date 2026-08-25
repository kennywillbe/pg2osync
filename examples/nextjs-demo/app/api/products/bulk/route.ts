import { NextRequest, NextResponse } from "next/server";
import { pool, randomProduct } from "@/lib/db";
import { countProducts, refreshIndex } from "@/lib/opensearch";
import { measureBulkPropagation } from "@/lib/propagation";

// Bulk operations exercise the engine's batching path: one statement touching
// many rows must arrive as one transaction and be flushed as a batch. The
// propagation numbers here are what dev/benchmark.sh measures at larger scale.

const MAX_BULK = 5000;

export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => ({}));
  const count = Math.min(Math.max(Number(body.count ?? 10), 1), MAX_BULK);

  const before = await countProducts() ?? 0;
  const rows = Array.from({ length: count }, randomProduct);

  // One multi-row INSERT is one WAL transaction — exactly what pg2osync's
  // transaction buffer exists to hold together.
  const values: unknown[] = [];
  const placeholders = rows
    .map((r, i) => {
      values.push(r.name, r.description, r.price, JSON.stringify(r.tags));
      const b = i * 4;
      return `($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}::jsonb)`;
    })
    .join(", ");

  const inserted = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags)
     VALUES ${placeholders}
     RETURNING id`,
    values,
  );
  const ids = inserted.rows.map((r: { id: number }) => r.id);

  const propagation = await measureBulkPropagation(ids, (_id, doc) =>
    doc !== null && rows.some((r) => r.name === doc.name),
  );

  return NextResponse.json({
    count,
    firstId: ids[0],
    lastId: ids[ids.length - 1],
    totalBefore: before,
    propagation,
  });
}

// Price update touches every row; verification compares each sampled document
// against its own pre-update price rather than assuming a uniform factor.
export async function PUT(req: NextRequest) {
  const body = await req.json().catch(() => ({}));
  const percent = Number(body.percent ?? 10);
  if (!Number.isFinite(percent) || percent === 0 || Math.abs(percent) > 90) {
    return NextResponse.json({ error: "percent must be nonzero, |p| <= 90" }, { status: 400 });
  }

  const targets = await pool.query("SELECT id, price FROM demo_products");
  if (targets.rowCount === 0) {
    return NextResponse.json({ updated: 0, percent, propagation: { landed: true, ms: 0, checked: 0 } });
  }
  const expected = new Map<number, number>(
    targets.rows.map((r: { id: number; price: string }) => [
      r.id,
      Math.round(parseFloat(r.price) * (1 + percent / 100) * 100) / 100,
    ]),
  );

  await pool.query(
    "UPDATE demo_products SET price = ROUND(price * (1 + $1 / 100.0), 2), updated_at = now()",
    [percent],
  );

  const propagation = await measureBulkPropagation(
    [...expected.keys()],
    (id, doc) => {
      if (doc === null) return false;
      const want = expected.get(Number(id));
      return want !== undefined && Math.abs(Number(doc.price) - want) < 0.005;
    },
    // A full-table update makes every document change at once, so pg2osync's
    // flush may take several batches; give it more room than a single write.
    { timeoutMs: 60_000 },
  );

  return NextResponse.json({ updated: expected.size, percent, propagation });
}

export async function DELETE(req: NextRequest) {
  const body = await req.json().catch(() => ({}));

  let ids: number[] = [];
  if (Array.isArray(body.ids)) {
    const parsed: number[] = body.ids.map((v: unknown) => Number(v));
    ids = [...new Set(parsed.filter((n) => Number.isInteger(n)))];
  } else if (body.oldest !== undefined) {
    const n = Math.min(Math.max(Number(body.oldest ?? 1), 1), MAX_BULK);
    const oldest = await pool.query(
      "SELECT id FROM demo_products ORDER BY id LIMIT $1",
      [n],
    );
    ids = oldest.rows.map((r: { id: number }) => r.id);
  }
  if (ids.length === 0) {
    return NextResponse.json({ error: "provide ids[] or oldest" }, { status: 400 });
  }

  await pool.query("DELETE FROM demo_products WHERE id = ANY($1::int[])", [ids]);

  const propagation = await measureBulkPropagation(ids, (_id, doc) => doc === null);

  return NextResponse.json({ deleted: ids.length, propagation });
}

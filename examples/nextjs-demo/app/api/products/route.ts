import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { getProductDoc, refreshIndex } from "@/lib/opensearch";
import { measurePropagation } from "@/lib/propagation";

// Every route in this file talks to PostgreSQL only. The propagation timing
// below reads OpenSearch purely to observe when pg2osync's write shows up —
// it never issues that write itself.

export async function GET() {
  const { rows } = await pool.query(
    "SELECT id, name, description, price, tags, updated_at FROM demo_products ORDER BY id",
  );
  return NextResponse.json({ products: rows });
}

export async function POST(req: NextRequest) {
  const body = await req.json();
  const name = String(body.name ?? "").trim();
  if (!name) {
    return NextResponse.json({ error: "name is required" }, { status: 400 });
  }
  const description = String(body.description ?? "");
  const price = Number(body.price ?? 0);
  const tags = Array.isArray(body.tags) ? body.tags : [];

  const { rows } = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags)
     VALUES ($1, $2, $3, $4::jsonb)
     RETURNING id, name, description, price, tags, updated_at`,
    [name, description, price, JSON.stringify(tags)],
  );
  const product = rows[0];

  const propagation = await measurePropagation(async () => {
    const doc = await getProductDoc(product.id);
    return doc !== null && doc.name === product.name;
  });
  await refreshIndex();

  return NextResponse.json({ product, propagation });
}

import { NextRequest, NextResponse } from "next/server";
import { listProducts, listReviews, pool } from "@/lib/db";
import { getProductDoc } from "@/lib/opensearch";
import { waitUntilSearchable } from "@/lib/propagation";

// Every route in this file talks to PostgreSQL only. The propagation timing
// below reads OpenSearch purely to observe when pg2osync's write shows up —
// it never issues that write itself.

export async function GET() {
  const [products, reviews] = await Promise.all([listProducts(), listReviews()]);
  return NextResponse.json({ products, reviews });
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
  // Fields the sync config deliberately keeps out of or transforms inside
  // OpenSearch; the document inspector is what makes that visible.
  const internalNote = String(body.internalNote ?? "");
  const supplierEmail = String(body.supplierEmail ?? "");

  const { rows } = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags, internal_note, supplier_email)
     VALUES ($1, $2, $3, $4::jsonb, $5, $6)
     RETURNING id, name, description, price, tags, updated_at`,
    [name, description, price, JSON.stringify(tags), internalNote, supplierEmail],
  );
  const product = rows[0];

  const propagation = await waitUntilSearchable();

  return NextResponse.json({ product, propagation });
}

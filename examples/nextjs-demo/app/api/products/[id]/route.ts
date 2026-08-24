import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { getProductDoc, refreshIndex } from "@/lib/opensearch";
import { measurePropagation } from "@/lib/propagation";

export async function PUT(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const body = await req.json();
  const name = String(body.name ?? "").trim();
  if (!name) {
    return NextResponse.json({ error: "name is required" }, { status: 400 });
  }
  const description = String(body.description ?? "");
  const price = Number(body.price ?? 0);
  const tags = Array.isArray(body.tags) ? body.tags : [];

  const { rows } = await pool.query(
    `UPDATE demo_products
     SET name = $2, description = $3, price = $4, tags = $5::jsonb, updated_at = now()
     WHERE id = $1
     RETURNING id, name, description, price, tags, updated_at`,
    [id, name, description, price, JSON.stringify(tags)],
  );
  if (rows.length === 0) {
    return NextResponse.json({ error: "not found" }, { status: 404 });
  }
  const product = rows[0];

  const propagation = await measurePropagation(async () => {
    const doc = await getProductDoc(product.id);
    return doc !== null && doc.name === product.name && doc.description === product.description;
  });
  await refreshIndex();

  return NextResponse.json({ product, propagation });
}

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const { rowCount } = await pool.query("DELETE FROM demo_products WHERE id = $1", [id]);
  if (rowCount === 0) {
    return NextResponse.json({ error: "not found" }, { status: 404 });
  }

  const propagation = await measurePropagation(async () => {
    const doc = await getProductDoc(id);
    return doc === null;
  });
  await refreshIndex();

  return NextResponse.json({ ok: true, propagation });
}

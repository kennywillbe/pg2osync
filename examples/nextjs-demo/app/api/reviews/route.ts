import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { getProductDoc, refreshIndex } from "@/lib/opensearch";
import { measurePropagation } from "@/lib/propagation";

// Reviews are a nested child collection: pg2osync embeds them as the
// `reviews` array on the parent product document and re-fetches the parent
// whenever a child changes. The check below verifies the embedding itself,
// not just that some document exists.
export async function POST(req: NextRequest) {
  const body = await req.json().catch(() => ({}));
  const productId = Number(body.productId);
  const author = String(body.author ?? "").trim();
  const rating = Math.min(Math.max(Number(body.rating ?? 5), 1), 5);
  const comment = String(body.comment ?? "");

  if (!Number.isInteger(productId) || !author) {
    return NextResponse.json({ error: "productId and author are required" }, { status: 400 });
  }

  const inserted = await pool.query(
    `INSERT INTO demo_reviews (product_id, author, rating, comment)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [productId, author, rating, comment],
  );
  const reviewId = inserted.rows[0].id as number;

  const propagation = await measurePropagation(async () => {
    const doc = await getProductDoc(productId);
    const reviews = doc?.reviews;
    return Array.isArray(reviews) && reviews.some((r) => r?.id === reviewId);
  });
  await refreshIndex();

  return NextResponse.json({ reviewId, productId, propagation });
}

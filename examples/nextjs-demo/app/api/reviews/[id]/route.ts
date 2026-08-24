import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { getProductDoc, refreshIndex } from "@/lib/opensearch";
import { measurePropagation } from "@/lib/propagation";

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const reviewId = Number(id);
  if (!Number.isInteger(reviewId)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }

  // The parent link is needed to verify the embedding afterwards; the child
  // delete carries no parent id in the WAL (REPLICA IDENTITY FULL supplies
  // the old row, pg2osync maps it back to the parent document).
  const target = await pool.query(
    "SELECT product_id FROM demo_reviews WHERE id = $1",
    [reviewId],
  );
  if (target.rowCount === 0) {
    return NextResponse.json({ error: "not found" }, { status: 404 });
  }
  const productId = target.rows[0].product_id as number;

  await pool.query("DELETE FROM demo_reviews WHERE id = $1", [reviewId]);

  const propagation = await measurePropagation(async () => {
    const doc = await getProductDoc(productId);
    const reviews = doc?.reviews;
    return Array.isArray(reviews) && !reviews.some((r) => r?.id === reviewId);
  });
  await refreshIndex();

  return NextResponse.json({ deletedId: reviewId, productId, propagation });
}

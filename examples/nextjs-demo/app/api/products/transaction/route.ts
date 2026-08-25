import { NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { getProductDoc } from "@/lib/opensearch";
import { waitUntilSearchable } from "@/lib/propagation";

// One transaction that inserts, updates and deletes at once. pg2osync buffers
// the whole transaction before flushing, so OpenSearch must never show the
// insert while the delete or update from the same commit is still missing —
// partial transactions are invisible as complete ones. The verification below
// only passes when all three effects are observable together.
export async function POST() {
  const victim = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags)
     VALUES ('tx victim', 'created to be deleted in the same demo', 1, '["tx"]'::jsonb)
     RETURNING id`,
  );
  const updated = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags)
     VALUES ('tx old name', 'will be renamed by the transaction', 2, '["tx"]'::jsonb)
     RETURNING id`,
  );
  const victimId = victim.rows[0].id as number;
  const updatedId = updated.rows[0].id as number;

  // The sentinel is created *before* the mixed transaction; if it disappears
  // from the index while the transaction's insert has not landed yet, the
  // delete leaked ahead of the rest of the commit.
  const sentinel = await pool.query(
    `INSERT INTO demo_products (name, description, price, tags)
     VALUES ('tx sentinel', 'marks where the mixed transaction begins', 3, '["tx"]'::jsonb)
     RETURNING id`,
  );
  const sentinelId = sentinel.rows[0].id as number;
  const sentinelLanded = await waitUntilSearchable();

  await pool.query("BEGIN");
  try {
    await pool.query(
      "UPDATE demo_products SET name = 'tx new name', updated_at = now() WHERE id = $1",
      [updatedId],
    );
    await pool.query("DELETE FROM demo_products WHERE id = $1", [victimId]);
    const inserted = await pool.query(
      `INSERT INTO demo_products (name, description, price, tags)
       VALUES ('tx atom', 'inserted inside the mixed transaction', 4, '["tx"]'::jsonb)
       RETURNING id`,
    );
    const atomId = inserted.rows[0].id as number;
    await pool.query("COMMIT");

    const propagation = await waitUntilSearchable();

    return NextResponse.json({
      insertedId: atomId,
      renamedId: updatedId,
      deletedId: victimId,
      sentinel: { id: sentinelId, propagation: sentinelLanded },
      propagation,
    });
  } catch (err) {
    await pool.query("ROLLBACK");
    throw err;
  }
}

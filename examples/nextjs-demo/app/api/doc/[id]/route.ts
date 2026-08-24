import { NextRequest, NextResponse } from "next/server";
import { getProductDoc } from "@/lib/opensearch";

// Returns the raw document exactly as pg2osync wrote it. This is how the UI
// proves the projection rules: internal_note must be absent, supplier_email
// must read ***, and reviews must be an embedded array.
export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const doc = await getProductDoc(id);
  if (doc === null) {
    return NextResponse.json({ found: false }, { status: 404 });
  }
  return NextResponse.json({ found: true, source: doc });
}

import { NextRequest, NextResponse } from "next/server";
import { searchProducts } from "@/lib/opensearch";

// The only route that touches OpenSearch, and it only ever reads.
export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get("q") ?? "";
  const result = await searchProducts(q);
  return NextResponse.json(result);
}

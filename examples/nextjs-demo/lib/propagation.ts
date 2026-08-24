/**
 * Polls OpenSearch after a PostgreSQL write until the effect of that write is
 * visible, and reports how long it took. This is the whole point of the demo:
 * the number on screen is a measurement, not a claim.
 */
export async function measurePropagation(
  check: () => Promise<boolean>,
  { timeoutMs = 15_000, intervalMs = 100 } = {},
): Promise<{ landed: boolean; ms: number }> {
  const start = performance.now();
  const deadline = start + timeoutMs;

  while (true) {
    if (await check()) {
      return { landed: true, ms: Math.round(performance.now() - start) };
    }
    if (performance.now() >= deadline) {
      return { landed: false, ms: Math.round(performance.now() - start) };
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

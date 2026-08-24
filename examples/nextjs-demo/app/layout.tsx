import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "pg2osync demo",
  description: "Write to PostgreSQL, watch it land in OpenSearch.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

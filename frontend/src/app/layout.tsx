import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'MovieMatcher — AI Movie Night',
  description: 'Find the perfect movie for everyone using AI-powered search',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-surface-0">{children}</body>
    </html>
  );
}

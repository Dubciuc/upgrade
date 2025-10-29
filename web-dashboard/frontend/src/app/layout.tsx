import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "@/theme/Providers";

export const metadata: Metadata = {
  title: "Weather Dashboard",
  description: "Real-time weather monitoring dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <Providers>
          {children}
        </Providers>
      </body>
    </html>
  );
}

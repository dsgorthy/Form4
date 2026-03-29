import type { Metadata } from "next";
import Script from "next/script";
import { ClerkProvider } from "@clerk/nextjs";
import { dark } from "@clerk/themes";
import { Inter, JetBrains_Mono } from "next/font/google";
import { Nav } from "@/components/nav";
import { TrialBanner } from "@/components/trial-banner";
import { Footer } from "@/components/footer";
import { OnboardingGuard } from "@/components/onboarding-guard";
import { TooltipProvider } from "@/components/ui/tooltip";
import "./globals.css";

const inter = Inter({
  variable: "--font-sans",
  subsets: ["latin"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

const isSandbox = process.env.NEXT_PUBLIC_API_URL?.includes("sandbox");

export const metadata: Metadata = {
  metadataBase: new URL(isSandbox ? "https://sandbox.form4.app" : "https://form4.app"),
  title: isSandbox ? "Form4 Sandbox" : "Form4 — Insider Intelligence, Decoded",
  description: "Real-time SEC Form 4 insider trade alerts with AI-powered signal grading. Track what insiders are buying and selling before the market reacts.",
  icons: {
    icon: [
      { url: isSandbox ? "/favicon-sandbox-32x32.png" : "/favicon-32x32.png", sizes: "32x32", type: "image/png" },
      { url: isSandbox ? "/favicon-sandbox-16x16.png" : "/favicon-16x16.png", sizes: "16x16", type: "image/png" },
    ],
    apple: isSandbox ? "/icon-sandbox-192x192.png" : "/icon-192x192.png",
  },
  openGraph: {
    title: "Form4 — Insider Intelligence, Decoded",
    description: "Real-time SEC Form 4 insider trade alerts with AI-powered signal grading.",
    images: [{ url: "/og-image.png", width: 1200, height: 630 }],
    siteName: "Form4",
  },
  twitter: {
    card: "summary_large_image",
    title: "Form4 — Insider Intelligence, Decoded",
    description: "Real-time SEC Form 4 insider trade alerts with AI-powered signal grading.",
    images: ["/og-image.png"],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <ClerkProvider
      appearance={{
        baseTheme: dark,
        variables: {
          colorPrimary: "#3B82F6",
          colorBackground: "#12121A",
          colorInputBackground: "#1A1A26",
          colorText: "#E8E8ED",
          colorTextSecondary: "#8888A0",
          colorInputText: "#E8E8ED",
          colorTextOnPrimaryBackground: "#FFFFFF",
          colorInputPlaceholder: "#8888A0",
          colorNeutral: "#E8E8ED",
        },
        elements: {
          card: {
            backgroundColor: "#12121A",
            border: "1px solid #2A2A3A",
            color: "#E8E8ED",
          },
          headerTitle: { color: "#E8E8ED" },
          headerSubtitle: { color: "#8888A0" },
          socialButtonsBlockButton: {
            color: "#E8E8ED",
            borderColor: "#2A2A3A",
            backgroundColor: "#1A1A26",
          },
          socialButtonsBlockButtonText: { color: "#E8E8ED" },
          dividerText: { color: "#8888A0" },
          dividerLine: { backgroundColor: "#2A2A3A" },
          formFieldLabel: { color: "#E8E8ED" },
          formFieldInput: {
            backgroundColor: "#1A1A26",
            color: "#E8E8ED",
            borderColor: "#2A2A3A",
          },
          formFieldInputShowPasswordButton: { color: "#8888A0" },
          footerActionText: { color: "#8888A0" },
          footerActionLink: { color: "#3B82F6" },
          footer: { color: "#8888A0" },
          footerPagesLink: { color: "#8888A0" },
          internal: { color: "#8888A0" },
          identityPreviewText: { color: "#E8E8ED" },
          identityPreviewEditButton: { color: "#3B82F6" },
          formFieldAction: { color: "#3B82F6" },
          formFieldHintText: { color: "#8888A0" },
          formFieldSuccessText: { color: "#22C55E" },
          formFieldWarningText: { color: "#F59E0B" },
          formFieldErrorText: { color: "#EF4444" },
          alertText: { color: "#E8E8ED" },
          otpCodeFieldInput: {
            color: "#E8E8ED",
            borderColor: "#2A2A3A",
            backgroundColor: "#1A1A26",
          },
          formButtonPrimary: {
            backgroundColor: "#3B82F6",
            color: "#FFFFFF",
          },
          userButtonPopoverCard: {
            backgroundColor: "#12121A",
            border: "1px solid #2A2A3A",
          },
          userButtonPopoverActionButton: { color: "#E8E8ED" },
          userButtonPopoverActionButtonText: { color: "#E8E8ED" },
          userButtonPopoverActionButtonIcon: { color: "#8888A0" },
          userButtonPopoverFooter: { display: "none" },
        },
      }}
    >
      <html lang="en">
        <head>
          <Script
            src="https://www.googletagmanager.com/gtag/js?id=G-N0CVNME0X8"
            strategy="afterInteractive"
          />
          <Script id="gtag-init" strategy="afterInteractive">
            {`
              window.dataLayer = window.dataLayer || [];
              function gtag(){dataLayer.push(arguments);}
              gtag('js', new Date());
              gtag('config', 'G-N0CVNME0X8');
            `}
          </Script>
        </head>
        <body
          className={`${inter.variable} ${jetbrainsMono.variable} antialiased font-sans`}
        >
          <TooltipProvider>
            <OnboardingGuard />
            <Nav />
            <TrialBanner />
            <main className="mx-auto max-w-7xl px-4 py-4 md:px-6 md:py-6">{children}</main>
            <Footer />
          </TooltipProvider>
        </body>
      </html>
    </ClerkProvider>
  );
}

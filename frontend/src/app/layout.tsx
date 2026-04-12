import type { Metadata } from "next";
import { ClerkProvider } from "@clerk/nextjs";
import { dark } from "@clerk/themes";
import { Inter, JetBrains_Mono } from "next/font/google";
import { Nav } from "@/components/nav";
import { TrialBanner } from "@/components/trial-banner";
import { Footer } from "@/components/footer";
import { OnboardingGuard } from "@/components/onboarding-guard";
import { AnalyticsProvider } from "@/components/analytics-provider";
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
  title: {
    default: isSandbox ? "Form4 Sandbox" : "Form4 — Live Insider Trading Strategies",
    template: isSandbox ? "%s — Form4 Sandbox" : "%s — Form4",
  },
  description: "Three live insider-trading strategies on real paper accounts. Every trade is public, research-backed, and fully transparent. 1.6M+ trades analyzed.",
  icons: {
    icon: [
      { url: isSandbox ? "/favicon-sandbox-32x32.png" : "/favicon-32x32.png", sizes: "32x32", type: "image/png" },
      { url: isSandbox ? "/favicon-sandbox-16x16.png" : "/favicon-16x16.png", sizes: "16x16", type: "image/png" },
    ],
    apple: isSandbox ? "/icon-sandbox-192x192.png" : "/icon-192x192.png",
  },
  openGraph: {
    title: "Form4 — Live Insider Trading Strategies",
    description: "Three live insider-trading strategies, publicly transparent. Every trade, every exit, every equity curve.",
    images: [{ url: "/og-image.png", width: 1200, height: 630 }],
    siteName: "Form4",
  },
  twitter: {
    card: "summary_large_image",
    title: "Form4 — Live Insider Trading Strategies",
    description: "Three live insider-trading strategies, publicly transparent. Every trade, every exit, every equity curve.",
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
          // UserProfile (Manage Account modal)
          userProfilePage: { backgroundColor: "#0A0A0F" },
          profilePage: { backgroundColor: "#0A0A0F" },
          page: { backgroundColor: "#0A0A0F", color: "#E8E8ED" },
          pageScrollBox: { backgroundColor: "#0A0A0F" },
          rootBox: { backgroundColor: "#0A0A0F" },
          modalContent: { backgroundColor: "#0A0A0F" },
          modalBackdrop: { backgroundColor: "rgba(0,0,0,0.7)" },
          navbar: {
            backgroundColor: "#0A0A0F",
            borderRight: "1px solid #2A2A3A",
          },
          navbarButton: { color: "#8888A0" },
          navbarButtonIcon: { color: "#8888A0" },
          navbarMobileMenuButton: { color: "#E8E8ED" },
          profileSectionTitle: { color: "#E8E8ED", borderBottom: "1px solid #2A2A3A" },
          profileSectionTitleText: { color: "#E8E8ED" },
          profileSectionContent: { color: "#E8E8ED" },
          profileSectionPrimaryButton: { color: "#3B82F6" },
          accordionTriggerButton: { color: "#E8E8ED" },
          accordionContent: { backgroundColor: "#12121A", color: "#E8E8ED" },
          badge: { backgroundColor: "#1A1A26", color: "#8888A0", border: "1px solid #2A2A3A" },
          tagInputContainer: { backgroundColor: "#1A1A26", borderColor: "#2A2A3A" },
          activeDeviceIcon: { color: "#22C55E" },
          menuButton: { color: "#8888A0" },
          menuList: { backgroundColor: "#12121A", border: "1px solid #2A2A3A" },
          menuItem: { color: "#E8E8ED" },
          // Catch-all for text elements Clerk renders inside the profile modal
          formFieldLabelRow__error: { color: "#EF4444" },
          tableHead: { color: "#8888A0" },
          providerIcon__google: { filter: "brightness(0.9)" },
        },
      }}
    >
      <html lang="en">
        <body
          className={`${inter.variable} ${jetbrainsMono.variable} antialiased font-sans`}
        >
          <AnalyticsProvider>
            <TooltipProvider>
              <OnboardingGuard />
              <Nav />
              <TrialBanner />
              <main className="mx-auto max-w-7xl px-4 py-4 md:px-6 md:py-6">{children}</main>
              <Footer />
            </TooltipProvider>
          </AnalyticsProvider>
        </body>
      </html>
    </ClerkProvider>
  );
}

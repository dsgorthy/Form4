import { dark } from "@clerk/themes";

export const clerkAppearance = {
  baseTheme: dark,
  variables: {
    colorPrimary: "#3B82F6",
    colorBackground: "#12121A",
    colorInputBackground: "#1A1A26",
    colorText: "#E8E8ED",
    colorTextSecondary: "#8888A0",
    colorInputText: "#E8E8ED",
    colorNeutral: "#E8E8ED",
  },
  elements: {
    rootBox: { margin: "0 auto" },
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
    footerActionText: { color: "#8888A0" },
    footerActionLink: { color: "#3B82F6" },
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
  },
};

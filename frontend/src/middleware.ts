import { clerkMiddleware } from "@clerk/nextjs/server";

// Permissive middleware — no routes are blocked.
// Onboarding redirect is handled client-side via OnboardingGuard component
// because Clerk doesn't include unsafeMetadata in JWT claims by default.
export default clerkMiddleware();

export const config = {
  matcher: [
    // Skip Next.js internals and static files
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    // Always run for API routes
    "/(api|trpc)(.*)",
  ],
};

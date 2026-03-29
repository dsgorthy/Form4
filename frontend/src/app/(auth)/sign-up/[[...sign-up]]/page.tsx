import { SignUp } from "@clerk/nextjs";
import { clerkAppearance } from "@/lib/clerk-appearance";

export default function SignUpPage() {
  return (
    <div className="flex min-h-[60vh] items-center justify-center">
      <SignUp appearance={clerkAppearance} />
    </div>
  );
}

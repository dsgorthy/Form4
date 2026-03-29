import { redirect } from "next/navigation";

interface Props {
  searchParams: Promise<{
    min_value?: string;
    min_tier?: string;
    page?: string;
  }>;
}

export default async function SellsRedirect({ searchParams }: Props) {
  const sp = await searchParams;
  const params = new URLSearchParams();
  params.set("trade_type", "sell");
  if (sp.min_value) params.set("min_value", sp.min_value);
  if (sp.min_tier) params.set("min_tier", sp.min_tier);
  if (sp.page) params.set("page", sp.page);
  redirect(`/feed?${params.toString()}`);
}

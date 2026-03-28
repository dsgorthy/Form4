#!/usr/bin/env python3
"""Generate Form4 corrected wordmark logos using Gemini image generation API.

Key concept: The word "Form4" uses the stylized arrow-4 AS the "4" character —
one integrated logotype, not a separate icon + text.
"""

import os
import sys
from google import genai
from google.genai import types

API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    print("Error: GEMINI_API_KEY not set")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

OUTPUT_DIR = "/Users/openclaw/trading-framework/brand"

INTEGRATED_4_DESC = """The number "4" at the end of "Form4" is NOT a plain text character. It is a stylized blue (#3B82F6) bold numeral "4" where the diagonal stroke extends upward into an arrow tip, symbolizing growth. This arrow-4 serves as both the logo mark AND the literal "4" in the word "Form4". There must be only ONE "4" visible — it is the last character of the word "Form4", rendered as the blue arrow-4 icon. The letters "F", "o", "r", "m" are in white bold sans-serif font. The "4" is in blue (#3B82F6) with the upward arrow integrated into its diagonal stroke. Together they read as the single word "Form4"."""

prompts = {
    "wordmark_horizontal_v2.png": f"""Create a horizontal wordmark logo image.

Dark background color #0A0A0F filling the entire image.
The text reads "Form4" as a single word, horizontally centered.
- The letters "Form" are in white (#FFFFFF), bold, modern sans-serif typeface (like Inter, Helvetica Neue, or SF Pro Display bold).
- {INTEGRATED_4_DESC}

Below the "Form4" wordmark, in smaller text, centered, is the tagline: "Insider Intelligence, Decoded" in medium gray (#6B7280), using a lighter weight of the same sans-serif font.

CRITICAL RULES:
- There is NO separate icon or logo mark to the left or right. The arrow-4 IS the logo, integrated as the "4" in "Form4".
- The number 4 appears exactly ONCE — as the last character of "Form4".
- Do NOT show "Form4" text next to a separate "4" icon. That would create two 4s.
- The image should be roughly 1200x400 pixels, landscape orientation.
- Clean, professional fintech branding. No decorative elements, no borders, no gradients on the background.""",

    "wordmark_stacked_v2.png": f"""Create a stacked/vertical wordmark logo image.

Dark background color #0A0A0F filling the entire image.
The text "Form4" is large, bold, and centered horizontally in the upper portion.
- The letters "Form" are in white (#FFFFFF), bold, modern sans-serif typeface.
- {INTEGRATED_4_DESC}

Below "Form4", with some vertical spacing, is the tagline "Insider Intelligence, Decoded" in smaller medium gray (#6B7280) text, also centered.

CRITICAL RULES:
- There is NO separate icon above or beside the text. The arrow-4 IS the logo, integrated as the "4" in "Form4".
- The number 4 appears exactly ONCE — as the last character of "Form4".
- Do NOT show a separate "4" icon above the text. That would create two 4s.
- The image should be roughly 800x600 pixels, portrait/square orientation.
- Clean, professional fintech branding. Minimalist. No decorative elements.""",

    "social_banner_x_v2.png": f"""Create a Twitter/X header banner image, exactly 1500x500 pixels.

Dark background color #0A0A0F filling the entire image.
The "Form4" wordmark is positioned center-left of the banner.
- The letters "Form" are in white (#FFFFFF), bold, large modern sans-serif typeface.
- {INTEGRATED_4_DESC}

To the right of "Form4" (with some spacing), the tagline "Insider Intelligence, Decoded" appears in medium gray (#6B7280), vertically centered with the wordmark, in a smaller font.

There is a very subtle blue glow or ambient light effect behind the "Form4" text — just enough to add depth without being flashy. Think a soft radial gradient of dark blue (#1E3A5F at maybe 20% opacity) emanating from behind the text.

CRITICAL RULES:
- There is NO separate icon anywhere in the banner. The arrow-4 IS the logo, integrated as the "4" in "Form4".
- The number 4 appears exactly ONCE — as the last character of "Form4".
- Do NOT place a separate "4" icon to the left of the text or anywhere else.
- Dimensions must be 1500x500 (3:1 ratio), suitable for X/Twitter header.
- Professional fintech branding. Minimal and bold.""",
}

config = types.GenerateContentConfig(
    response_modalities=["TEXT", "IMAGE"],
)

for filename, prompt in prompts.items():
    output_path = os.path.join(OUTPUT_DIR, filename)
    print(f"\nGenerating {filename}...")
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-image",
            contents=prompt,
            config=config,
        )
        saved = False
        for part in response.parts:
            if part.inline_data is not None:
                image = part.as_image()
                image.save(output_path)
                print(f"  Saved to {output_path}")
                saved = True
                break
            elif part.text is not None:
                print(f"  Text response: {part.text[:300]}")
        if not saved:
            print(f"  WARNING: No image generated for {filename}")
    except Exception as e:
        print(f"  ERROR: {e}")

print("\nDone!")

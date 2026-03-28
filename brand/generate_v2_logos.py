#!/usr/bin/env python3
"""Generate Form4 logo v2 variations using Gemini image generation API."""

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

# Base prompt elements shared across all variations
BASE_CONTEXT = """Design a minimalist app icon logo for "Form4", a financial data product.
The icon is a rounded square (iOS app icon shape) on a pure black or near-black (#0A0A0F) background.
The design centers on a bold numeral "4" where the diagonal stroke of the 4 incorporates an upward-pointing arrow, symbolizing growth/upward movement.
Style: clean, flat design. No 3D effects, no shadows, no gradients on the background, no text other than the "4" itself.
The "4" must be instantly recognizable even at very small sizes (32x32 pixels).
Primary accent color: blue (#3B82F6).
The icon should look professional and modern, suitable for a fintech/trading application.
Output a single icon centered on a black background, nothing else."""

prompts = {
    "logo_v2_1.png": f"""{BASE_CONTEXT}

SPECIFIC VARIATION: The "4" with arrow has a gradient that transitions from dark blue (#2563EB) at the base to bright blue (#60A5FA) at the arrow tip pointing upward. This gives a sense of energy flowing upward. The rounded square background is dark (#0A0A0F). The "4" is bold and filled.""",

    "logo_v2_2.png": f"""{BASE_CONTEXT}

SPECIFIC VARIATION: The arrow is subtly integrated into the "4" — rather than having a distinct arrowhead, the diagonal stroke of the "4" simply tapers and angles upward naturally, so it reads as both a "4" and an upward gesture simultaneously. No separate arrowhead shape. The stroke just flows upward. Bold blue (#3B82F6) on dark (#0A0A0F) rounded square.""",

    "logo_v2_3.png": f"""{BASE_CONTEXT}

SPECIFIC VARIATION: The "4" with upward arrow is rendered in a thin line/stroke style rather than bold filled shapes. Think 2-3px stroke weight. Very modern and minimal, like a wireframe icon. The lines are blue (#3B82F6) on the dark (#0A0A0F) rounded square background. Elegant and lightweight feeling.""",

    "logo_v2_4.png": f"""{BASE_CONTEXT}

SPECIFIC VARIATION: INVERTED color scheme within the icon. The rounded square background itself is a rich blue gradient (from #2563EB to #3B82F6), and the "4" with upward arrow is WHITE or very light (#F8FAFC) on top of this blue background. The overall image background outside the rounded square is black (#000000). The white "4" on blue creates strong contrast.""",

    "logo_v2_5.png": f"""{BASE_CONTEXT}

SPECIFIC VARIATION: The bold blue (#3B82F6) "4" with upward arrow on dark (#0A0A0F) rounded square, PLUS a small bright green (#22C55E) circle dot in the bottom-right corner of the icon, like a status indicator showing "active" or "live". The green dot should be small (about 10-15% of the icon size) and positioned in the lower-right area of the rounded square. It suggests the system is live and monitoring.""",
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
                print(f"  Text response: {part.text[:200]}")
        if not saved:
            print(f"  WARNING: No image generated for {filename}")
    except Exception as e:
        print(f"  ERROR: {e}")

print("\nDone!")

#!/usr/bin/env python3
"""
Name standardization for insider catalog.

SEC EDGAR filings store names in "LAST FIRST MIDDLE" format, often all-uppercase.
This script creates a `display_name` column with human-readable formatting:
  "NADELLA SATYA" → "Satya Nadella"
  "DELL MICHAEL S" → "Michael S. Dell"
  "Komin Robert Patrick Jr." → "Robert Patrick Komin Jr."

Entity names (LLCs, trusts, funds) are title-cased if all-uppercase,
otherwise left as-is.

Usage:
  python name_cleaner.py                # Clean all names
  python name_cleaner.py --dry-run      # Preview changes
  python name_cleaner.py --verify       # Cross-reference top insiders with SEC EDGAR
  python name_cleaner.py --stats        # Show cleaning stats
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

# Name suffixes that should stay at the end
# Note: single-letter "V" excluded — almost always a middle initial in EDGAR data.
# Multi-char Roman numerals (II, III, IV, VI+) are unambiguous suffixes.
SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "vi", "vii", "viii",
            "md", "m.d.", "phd", "ph.d.", "esq", "esq.", "cpa", "cfa"}

# Words that should stay lowercase in entity names
LOWERCASE_WORDS = {"of", "the", "and", "for", "in", "on", "at", "to", "by", "an", "a"}

# Words that should stay uppercase in entity names (acronyms)
ACRONYM_PATTERNS = re.compile(
    r'^[A-Z]{2,5}$'  # 2-5 letter all-caps likely an acronym
)

# Prefixes that are part of last names (kept together)
LAST_NAME_PREFIXES = {"mc", "mac", "de", "van", "von", "di", "le", "la", "du", "del",
                      "al", "el", "o'", "der", "den", "des", "het", "ten", "ter", "das", "dos"}

# --- compound surnames -------------------------------------------------------
#
# EDGAR gives "LAST FIRST MIDDLE", and rotating token 0 to the end is right
# only when the surname is one token. It is wrong for a compound surname:
#
#   "SIMMONS SMITH CATHERINE A"  rotate -> "Smith Catherine A. Simmons"  WRONG
#                                correct -> "Catherine A. Simmons Smith"
#
# Telling the two apart needs population frequency, which is frozen into
# name_lexicon.json by scripts/build_name_lexicon.py (see that file for how
# it is learned). A token absent from both lists means "no opinion" and the
# default rotation stands, so a stale or missing lexicon degrades to today's
# behaviour rather than misfiring.
_LEXICON_PATH = Path(__file__).resolve().parent / "name_lexicon.json"


def _load_lexicon() -> tuple[frozenset, frozenset]:
    try:
        import json
        d = json.loads(_LEXICON_PATH.read_text())
        return frozenset(d.get("given", ())), frozenset(d.get("surname", ()))
    except Exception:  # missing/corrupt -> no compound detection, not a crash
        logger.warning("name_lexicon.json unreadable; compound-surname detection off")
        return frozenset(), frozenset()


GIVEN_TOKENS, SURNAME_TOKENS = _load_lexicon()

# Chinese/Korean/Vietnamese names arrive surname-first already, so the plain
# rotation is correct for them: "KONG XIAO JUN" -> "Xiao Jun Kong". The
# frequency signal cannot see this — "Xiao" reads surname-like in isolation —
# and would produce the nonsense "Jun Kong Xiao", so skip on the surname
# itself. Covers the high-frequency cases present in the filing set.
CJK_SURNAMES = {
    "wang", "li", "zhang", "liu", "chen", "yang", "huang", "zhao", "wu", "zhou",
    "xu", "sun", "ma", "zhu", "hu", "guo", "he", "gao", "lin", "luo", "zheng",
    "liang", "xie", "song", "tang", "han", "feng", "deng", "cao", "peng", "zeng",
    "xiao", "tian", "dong", "pan", "yuan", "cai", "jiang", "yu", "ye", "shen",
    "kong", "leung", "wong", "chan", "cheung", "lau", "ng", "ho", "chow", "tsang",
    "kim", "park", "choi", "jung", "kang", "yoon", "lim", "shin", "seo", "kwon",
    "nguyen", "tran", "pham", "vu", "vo", "dang", "bui", "do", "ngo", "duong",
    "tan", "goh", "teo", "ong", "sim", "koh", "yeo", "toh",
}


def _norm_token(tok: str) -> str:
    return tok.lower().strip(".,'\"")


def _has_compound_surname(tokens: list[str]) -> bool:
    """True when tokens[0:2] form one surname rather than LAST + FIRST.

    Expects suffixes already stripped. Two shapes qualify:
        [LAST1 LAST2 FIRST]          "Simmons Smith Catherine"
        [LAST1 LAST2 FIRST INITIAL]  "Simmons Smith Catherine A."

    Both signals must agree — the given-name slot looks like a given name AND
    the second surname slot looks like a surname. "SMITH JOHN MICHAEL" has two
    given-ish tokens and so stays on the default rotation. Deliberately
    conservative: declining is free, flipping a correct name is a visible
    regression.
    """
    if len(tokens) == 4:
        if not (_is_middle_initial(tokens[3]) and not _is_middle_initial(tokens[2])):
            return False
    elif len(tokens) != 3:
        return False

    a, b, c = tokens[0], tokens[1], tokens[2]
    if _is_middle_initial(b) or _is_middle_initial(c):
        return False
    if _norm_token(a) in CJK_SURNAMES:
        return False
    if _norm_token(a) in LAST_NAME_PREFIXES or _norm_token(b) in LAST_NAME_PREFIXES:
        return False  # particle logic below owns these
    return _norm_token(c) in GIVEN_TOKENS and _norm_token(b) in SURNAME_TOKENS


def _smart_title_case(word: str) -> str:
    """Title-case a word, handling special patterns like McDonald, O'Brien."""
    lower = word.lower()

    # Already has mixed case and looks intentional (e.g. "McDonald")
    if not word.isupper() and not word.islower() and len(word) > 2:
        return word

    # O'Something / D'Something pattern
    if lower[:2] in ("o'", "d'") and len(lower) > 2:
        return lower[0].upper() + "'" + lower[2:].capitalize()

    # Mc/Mac prefix
    if lower.startswith("mc") and len(lower) > 2:
        return "Mc" + lower[2:].capitalize()
    if lower.startswith("mac") and len(lower) > 3 and lower != "mace":
        return "Mac" + lower[3:].capitalize()

    return word.capitalize()


def _is_middle_initial(token: str) -> bool:
    """Check if a token is a middle initial (single letter, optionally with period)."""
    return bool(re.match(r'^[A-Za-z]\.?$', token))


def _format_suffix(suffix: str) -> str:
    """Standardize suffix formatting."""
    s = suffix.lower().rstrip('.')
    mapping = {
        "jr": "Jr.", "sr": "Sr.",
        "ii": "II", "iii": "III", "iv": "IV",
        "vi": "VI", "vii": "VII", "viii": "VIII",
        "md": "M.D.", "m.d": "M.D.",
        "phd": "Ph.D.", "ph.d": "Ph.D.",
        "esq": "Esq.", "cpa": "CPA", "cfa": "CFA",
    }
    return mapping.get(s, suffix)


def clean_person_name(raw_name: str) -> str:
    """
    Convert EDGAR "LAST FIRST MIDDLE [SUFFIX]" to "First Middle Last [Suffix]".

    Examples:
      "NADELLA SATYA"       → "Satya Nadella"
      "DELL MICHAEL S"      → "Michael S. Dell"
      "SMITH BRADFORD L"    → "Bradford L. Smith"
      "Komin Robert Patrick Jr." → "Robert Patrick Komin Jr."
      "O'BRIEN JAMES M"    → "James M. O'Brien"
    """
    # EDGAR sometimes emits a comma between surname and given name
    # ("Barbe, Cohen Jana"). It is a separator, never part of the name, and
    # left in place it rides along into the reordered output as "Jana Barbe,
    # Cohen". Strip before tokenizing so downstream comparisons see bare words.
    tokens = [t for t in raw_name.replace(",", " ").strip().split() if t]
    if not tokens:
        return raw_name

    # Single word — just title-case it
    if len(tokens) == 1:
        return _smart_title_case(tokens[0])

    # Extract suffixes from the end
    suffixes = []
    while tokens and tokens[-1].lower().rstrip('.') in {s.rstrip('.') for s in SUFFIXES}:
        suffixes.insert(0, _format_suffix(tokens.pop()))

    if not tokens:
        return raw_name  # all suffixes, shouldn't happen

    # EDGAR also emits the suffix directly after the surname:
    # "Ruch, Jr. Charles E." -> once the comma is gone, ["Ruch","Jr.","Charles","E."].
    # The end-of-string loop above cannot see it, so it survived into the output
    # as a leading "Jr. Charles E. Ruch". Pull it out to join the real suffixes.
    _suffix_stems = {s.rstrip('.') for s in SUFFIXES}
    if len(tokens) >= 3 and tokens[1].lower().rstrip('.') in _suffix_stems:
        suffixes.insert(0, _format_suffix(tokens.pop(1)))

    # Fix EDGAR stripping apostrophes from O'Something and D'Something names
    # ("O HERN" → "O'Hern", "D AMBROSE" → "D'Ambrose"). A bare single-letter
    # surname is otherwise meaningless, so this is safe.
    if len(tokens) >= 2 and len(tokens[0]) == 1 and tokens[0].upper() in {"O", "D"}:
        tokens[0] = tokens[0].upper() + "'" + tokens[1]
        tokens.pop(1)

    # First token(s) are the last name in EDGAR format.
    # Handle multi-word last names with prefixes (DE LA CRUZ, VAN DER BERG, etc.)
    last_parts = [tokens[0]]
    i = 1
    # Compound surname with no particle to mark it ("SIMMONS SMITH CATHERINE").
    # Checked before the prefix branch, which handles the particle-marked form.
    if _has_compound_surname(tokens):
        last_parts = tokens[:2]
        i = 2
    # If first token is a prefix (DE, VAN, VON, etc.), consume following prefix/name tokens
    elif tokens[0].lower() in LAST_NAME_PREFIXES and len(tokens) > 2:
        while i < len(tokens) - 1:  # keep at least one token for first name
            if tokens[i].lower() in LAST_NAME_PREFIXES:
                last_parts.append(tokens[i])
                i += 1
            elif i == len(last_parts):
                # Next non-prefix token completes the last name
                last_parts.append(tokens[i])
                i += 1
                break
            else:
                break
    last_name = " ".join(
        t.lower() if t.lower() in LAST_NAME_PREFIXES else _smart_title_case(t)
        for t in last_parts
    )
    first_middle = tokens[i:]

    if not first_middle:
        # Only a last name
        parts = [last_name]
    else:
        # Format first and middle names
        formatted = []
        for t in first_middle:
            if _is_middle_initial(t):
                # Ensure middle initials have a period
                formatted.append(t.upper().rstrip('.') + '.')
            else:
                formatted.append(_smart_title_case(t))

        # Reassemble: First [Middle] Last [Suffix]
        parts = formatted + [last_name]

    if suffixes:
        parts.extend(suffixes)

    return " ".join(parts)


def clean_entity_name(raw_name: str) -> str:
    """
    Clean entity names (LLCs, trusts, etc.).
    Title-case if all-uppercase, otherwise preserve original casing.

    Examples:
      "TTWFGP LLC"                    → "Ttwfgp LLC"
      "RES Business Management LLC"   → "RES Business Management LLC" (unchanged)
      "BERKSHIRE HATHAWAY INC"        → "Berkshire Hathaway Inc."
    """
    # If not all-uppercase, it's probably already readable
    if not raw_name.isupper():
        return raw_name

    tokens = raw_name.split()
    result = []
    for token in tokens:
        lower = token.lower().rstrip('.')

        # Common entity suffixes — keep standard form
        entity_suffixes = {
            "llc": "LLC", "l.l.c": "LLC", "l.l.c.": "LLC",
            "lp": "L.P.", "l.p": "L.P.", "l.p.": "L.P.",
            "inc": "Inc.", "inc.": "Inc.",
            "corp": "Corp.", "corp.": "Corp.",
            "ltd": "Ltd.", "ltd.": "Ltd.",
            "plc": "PLC", "co": "Co.",
        }
        if lower in entity_suffixes:
            result.append(entity_suffixes[lower])
        elif ACRONYM_PATTERNS.match(token):
            result.append(token)  # keep uppercase acronyms
        elif lower in LOWERCASE_WORDS and result:  # not first word
            result.append(lower)
        else:
            result.append(_smart_title_case(token))

    return " ".join(result)


def clean_name(raw_name: str, is_entity: bool) -> str:
    """Route to person or entity name cleaner."""
    if not raw_name or not raw_name.strip():
        return raw_name
    if is_entity:
        return clean_entity_name(raw_name)
    return clean_person_name(raw_name)


def ensure_column(conn):
    """Add display_name column if it doesn't exist."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(insiders)").fetchall()}
    if "display_name" not in cols:
        conn.execute("ALTER TABLE insiders ADD COLUMN display_name TEXT")
        conn.commit()
        logger.info("Added display_name column to insiders table")


def run_cleaning(conn, dry_run: bool = False):
    """Clean all insider names and populate display_name."""
    if not dry_run:
        ensure_column(conn)

    rows = conn.execute(
        "SELECT insider_id, name, COALESCE(is_entity, 0) FROM insiders"
    ).fetchall()

    changed = 0
    unchanged = 0
    samples = []

    for insider_id, raw_name, is_entity in rows:
        display = clean_name(raw_name, bool(is_entity))

        if display != raw_name:
            changed += 1
            if len(samples) < 20:
                samples.append((raw_name, display, bool(is_entity)))
            if not dry_run:
                conn.execute(
                    "UPDATE insiders SET display_name = ? WHERE insider_id = ?",
                    (display, insider_id),
                )
        else:
            unchanged += 1
            if not dry_run:
                conn.execute(
                    "UPDATE insiders SET display_name = ? WHERE insider_id = ?",
                    (raw_name, insider_id),
                )

    if not dry_run:
        conn.commit()

    logger.info("Name cleaning: %d changed, %d unchanged, %d total", changed, unchanged, len(rows))

    if samples:
        print(f"\nSample changes ({min(len(samples), 20)} of {changed}):")
        for raw, clean, entity in samples:
            tag = " [entity]" if entity else ""
            print(f"  {raw:40s} → {clean}{tag}")
    print()


def verify_with_edgar(conn, limit: int = 50):
    """
    Cross-reference top insiders with SEC EDGAR CIK lookup.
    Uses the SEC's company/person search to verify names.
    """
    try:
        import urllib.request
        import json
    except ImportError:
        logger.error("urllib required for EDGAR verification")
        return

    # Get top insiders by score (most important to verify)
    rows = conn.execute("""
        SELECT i.insider_id, i.name, i.display_name, i.cik,
               COALESCE(i.is_entity, 0), itr.score, itr.score_tier
        FROM insiders i
        JOIN insider_track_records itr ON i.insider_id = itr.insider_id
        WHERE itr.score_tier >= 2 AND i.cik IS NOT NULL
        ORDER BY itr.score DESC
        LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        logger.info("No scored insiders with CIKs to verify")
        return

    logger.info("Verifying %d top insiders against SEC EDGAR...", len(rows))

    verified = 0
    mismatched = 0
    errors = 0

    for insider_id, raw_name, display_name, cik, is_entity, score, tier in rows:
        try:
            # SEC EDGAR CIK lookup — returns the official name for a CIK
            url = f"https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&dateRange=custom&startdt=2020-01-01&enddt=2026-01-01&forms=4"
            req = urllib.request.Request(url, headers={"User-Agent": "openclaw trading-framework research@openclaw.com"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())

            # Alternative: use the EDGAR company tickers JSON
            url2 = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
            req2 = urllib.request.Request(url2, headers={"User-Agent": "openclaw trading-framework research@openclaw.com"})
            with urllib.request.urlopen(req2, timeout=5) as resp2:
                sub_data = json.loads(resp2.read())

            edgar_name = sub_data.get("name", "")
            if edgar_name:
                # Compare normalized versions
                our_norm = raw_name.lower().strip()
                edgar_norm = edgar_name.lower().strip()

                if our_norm == edgar_norm or display_name and display_name.lower().strip() == edgar_norm:
                    verified += 1
                else:
                    mismatched += 1
                    logger.info(
                        "  MISMATCH T%d %s: ours='%s' display='%s' edgar='%s'",
                        tier, cik, raw_name, display_name, edgar_name,
                    )
            else:
                errors += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.warning("  EDGAR lookup failed for CIK %s: %s", cik, e)

    logger.info(
        "EDGAR verification: %d verified, %d mismatched, %d errors (of %d checked)",
        verified, mismatched, errors, len(rows),
    )


def print_stats(conn):
    """Print name cleaning statistics."""
    total = conn.execute("SELECT COUNT(*) FROM insiders").fetchone()[0]

    cols = {row[1] for row in conn.execute("PRAGMA table_info(insiders)").fetchall()}
    if "display_name" not in cols:
        print("display_name column not yet created. Run name_cleaner.py first.")
        return

    has_display = conn.execute(
        "SELECT COUNT(*) FROM insiders WHERE display_name IS NOT NULL"
    ).fetchone()[0]
    all_upper = conn.execute(
        "SELECT COUNT(*) FROM insiders WHERE name = UPPER(name)"
    ).fetchone()[0]
    entities = conn.execute(
        "SELECT COUNT(*) FROM insiders WHERE is_entity = 1"
    ).fetchone()[0]
    changed = conn.execute(
        "SELECT COUNT(*) FROM insiders WHERE display_name != name"
    ).fetchone()[0]

    print(f"\n{'='*50}")
    print("NAME CLEANING STATS")
    print(f"{'='*50}")
    print(f"Total insiders:      {total:,}")
    print(f"All-uppercase names: {all_upper:,} ({all_upper/total*100:.1f}%)")
    print(f"Entity insiders:     {entities:,}")
    print(f"Names cleaned:       {has_display:,}")
    print(f"Names changed:       {changed:,}")
    print(f"{'='*50}\n")


def main():
    parser = argparse.ArgumentParser(description="Standardize insider display names")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--verify", action="store_true", help="Cross-reference top insiders with SEC EDGAR")
    parser.add_argument("--verify-limit", type=int, default=50, help="Max insiders to verify (default: 50)")
    parser.add_argument("--stats", action="store_true", help="Show cleaning stats")
    args = parser.parse_args()

    conn = get_connection()

    if args.stats:
        print_stats(conn)
    elif args.verify:
        verify_with_edgar(conn, limit=args.verify_limit)
    else:
        run_cleaning(conn, dry_run=args.dry_run)

    conn.close()


if __name__ == "__main__":
    main()

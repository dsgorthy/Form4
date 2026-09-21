"""The watchdog fetches the pages Google sends people to, as Googlebot, and
checks they still say what Google needs: 200, a canonical, no noindex, a
rendered record; robots.txt with a sitemap and no blanket disallow; a real
sitemap index. Between 2026-09-16 and 09-20 the edge dropped connections
the home-page probe never saw, and Google visitors went to zero."""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "offbox_watchdog", Path(__file__).resolve().parents[2] / "scripts" / "offbox_watchdog.py",
)
w = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(w)

PAGE = "<html><head><link rel=\"canonical\" href=\"https://form4.app/insider/x\"/></head><body>" + "record " * 5000 + "</body></html>"


def test_a_healthy_page_passes():
    assert w.check_page("https://form4.app/insider/x", 200, PAGE) == []


def test_status_noindex_canonical_and_thin_render_are_named():
    assert w.check_page("https://form4.app/insider/x", 502, "") == ["HTTP 502"]
    assert "page carries noindex" in w.check_page("https://form4.app/insider/x", 200, PAGE.replace("<body>", '<meta name="robots" content="noindex"><body>'))
    assert "page has no canonical" in w.check_page("https://form4.app/insider/x", 200, PAGE.replace('rel="canonical"', 'rel="alternate"'))
    assert any("bytes" in p for p in w.check_page("https://form4.app/insider/x", 200, "<html>short</html>"))


def test_robots_and_sitemap_checks():
    assert w.check_page("https://form4.app/robots.txt", 200, "User-Agent: *\nAllow: /\nSitemap: https://form4.app/sitemap.xml\n") == []
    assert "robots.txt has no Sitemap line" in w.check_page("https://form4.app/robots.txt", 200, "User-Agent: *\nAllow: /\n")
    assert "robots.txt disallows everything" in w.check_page("https://form4.app/robots.txt", 200, "User-Agent: *\nDisallow: /")
    assert w.check_page("https://form4.app/sitemap.xml", 200, "<sitemapindex><sitemap><loc>x</loc></sitemap></sitemapindex>") == []
    assert w.check_page("https://form4.app/sitemap.xml", 200, "<urlset></urlset>")


def test_the_probes_cover_both_seo_surfaces_and_are_fetched_as_googlebot():
    urls = list(w.PAGE_PROBES.values())
    assert any("/insider/" in u for u in urls) and any("/company/" in u for u in urls)
    assert "Googlebot" in w.GOOGLEBOT_UA
    src = Path(w.__file__).read_text(encoding="utf-8")
    assert "for name, url in PAGE_PROBES.items():" in src

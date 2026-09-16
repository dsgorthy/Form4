"""The keepalive re-issues a forward for every host port the containers
publish. It learns them from `docker ps --format '{{.Ports}}'`, which mixes
published and unpublished ports on one comma-separated line per container.
Get the parse wrong and a port silently stays dead after the next master
death -- which is the outage this script exists to end."""
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "lima_master_keepalive.sh"

# Verbatim from the Studio, 2026-09-16.
DOCKER_PS_PORTS = """\
3000/tcp
8000/tcp
8000/tcp
3000/tcp
3000/tcp
3000/tcp
8000/tcp
443/tcp, 2019/tcp, 443/udp, 0.0.0.0:8082->80/tcp, [::]:8082->80/tcp
443/tcp, 2019/tcp, 443/udp, 0.0.0.0:8080->80/tcp, [::]:8080->80/tcp
0.0.0.0:80->80/tcp, [::]:80->80/tcp, 0.0.0.0:443->443/tcp, [::]:443->443/tcp, 443/udp, 2019/tcp
"""


def parse(text: str) -> list[str]:
    out = subprocess.run(
        ["bash", "-c", f"source '{SCRIPT}'; published_host_ports"],
        input=text, capture_output=True, text=True, check=True,
    )
    return out.stdout.split()


def test_published_ports_are_the_four_host_ports_once_each():
    assert parse(DOCKER_PS_PORTS) == ["80", "443", "8080", "8082"]


def test_unpublished_and_udp_ports_are_ignored():
    assert parse("3000/tcp\n443/udp, 2019/tcp\n") == []


def test_ipv6_only_publication_still_counts():
    assert parse("[::]:9090->80/tcp\n") == ["9090"]


def test_sourcing_the_script_runs_nothing():
    # Tests source it for the functions; the main loop must not start.
    out = subprocess.run(
        ["bash", "-c", f"source '{SCRIPT}'; echo sourced"],
        capture_output=True, text=True, timeout=10,
    )
    assert out.stdout.strip() == "sourced"


@pytest.mark.parametrize("line", ["0.0.0.0:80->80/tcp", " 0.0.0.0:80->80/tcp "])
def test_whitespace_around_an_entry_does_not_matter(line):
    assert parse(line + "\n") == ["80"]

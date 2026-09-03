# `studio` — the deploy CLI

The single entry point for everything that ships to the Mac Studio:
`studio deploy form4`, `studio status`, `studio logs`, `studio db-migrate`,
`studio ssh`, `studio health`.

## Why it lives here now

It did not. It existed only at `~/.local/bin/studio` on the Mini — 17KB of
deploy logic, no copy anywhere, no history. Losing that machine meant losing
the ability to deploy, and every fix made to it was invisible to review.

`~/.local/bin/studio` is still the executable that runs. This is the tracked
copy. After editing either one:

    cp scripts/bin/studio ~/.local/bin/studio     # repo  -> live
    cp ~/.local/bin/studio scripts/bin/studio     # live  -> repo

`tests/unit/test_studio_cli_is_tracked.py` fails the build when the two drift,
so the copy cannot quietly go stale.

## Contains no secrets

Only the Studio tailnet address (`100.78.9.66`, already in CLAUDE.md) and
paths. Credentials live in `.env` on the target host and in the macOS
Keychain; nothing is read into this file.

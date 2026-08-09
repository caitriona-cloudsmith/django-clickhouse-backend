#!/usr/bin/env python3
"""Set the release version everywhere it is written down.

Usage: python scripts/release.py 2.0.0
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATUSES = {"a": "alpha", "b": "beta", "rc": "rc"}
# Docs annotate a feature with the version it lands in, which is not decided
# yet while the feature is written, so they write NEXT and this fills it in.
PLACEHOLDER = "NEXT"


def parse(version):
    """Return the VERSION tuple source for a X.Y.Z, X.Y.ZaN, X.Y.ZbN or X.Y.ZrcN version."""
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?", version)
    if match is None:
        sys.exit(f"{version} is not a X.Y.Z, X.Y.ZaN, X.Y.ZbN or X.Y.ZrcN version")
    major, minor, patch, status, number = match.groups()
    status = STATUSES.get(status, "final")
    return f'({major}, {minor}, {patch}, "{status}", {number or 0})'


def substitute(path, pattern, replacement, flags=0, required=True):
    text = path.read_text()
    new_text, count = re.subn(pattern, replacement, text, flags=flags)
    name = path.relative_to(ROOT)
    if not count:
        if required:
            sys.exit(f"{name} has nothing matching {pattern}")
        return
    path.write_text(new_text)
    print(f"{name}: {count} occurrence(s) of {pattern}")


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    version = sys.argv[1]
    version_tuple = parse(version)

    # The changelog heading goes first: it is the only substitution that fails
    # on a second run, so nothing is written when the release is already done.
    substitute(
        ROOT / "CHANGELOG.md",
        r"^### Unreleased$",
        f"### {version}",
        flags=re.MULTILINE,
    )
    substitute(
        ROOT / "clickhouse_backend/__init__.py",
        r"^VERSION = .*$",
        f"VERSION = {version_tuple}",
        flags=re.MULTILINE,
    )
    for path in [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
        substitute(
            path,
            rf"(in version) {PLACEHOLDER}",
            rf"\1 {version}",
            required=False,
        )

    print(
        f"\nNext: git commit -am 'Bump version to {version}' && git tag v{version}"
        f"\nThen: git push origin main v{version}"
    )


if __name__ == "__main__":
    main()

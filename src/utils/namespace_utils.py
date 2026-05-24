from __future__ import annotations

import re
import urllib.parse
import urllib.request
from typing import Optional, Tuple


def split_iri(iri: str) -> Tuple[str, str]:
    """Split a full IRI into (namespace_prefix, local_name)."""
    if not iri:
        return ("", "")
    pos = iri.find("#")
    if pos >= 0:
        return iri[: pos + 1], iri[pos + 1 :]
    pos = iri.rfind("/")
    if pos >= 0:
        return iri[: pos + 1], iri[pos + 1 :]
    pos = iri.rfind(":")
    if pos >= 0:
        return iri[: pos + 1], iri[pos + 1 :]
    return iri, ""


def parse_prefix_cc_ttl(body: str) -> Optional[str]:
    """Extract a namespace abbreviation from a prefix.cc Turtle response."""
    match = re.search(r"@prefix\s+(\S+):\s+<[^>]+>\s*\.", body or "")
    if match:
        return match.group(1)
    return None


def lookup_prefix_cc_abbr(prefix_value: str, timeout: int = 5) -> Optional[str]:
    """Query prefix.cc reverse lookup to resolve a namespace abbreviation."""
    try:
        encoded = urllib.parse.quote(prefix_value, safe="")
        url = f"http://prefix.cc/reverse?uri={encoded}&format=ttl"
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            body = resp.read().decode("utf-8")
        return parse_prefix_cc_ttl(body)
    except Exception:
        return None

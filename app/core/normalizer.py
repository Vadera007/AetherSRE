"""
AetherSRE — High-Performance Regex Log Normalizer
===================================================
Log messages are saturated with volatile, high-cardinality tokens: IP
addresses, UUIDs, user IDs, transaction IDs, timestamps, and memory
addresses.  These tokens are catastrophic for vector similarity because
two semantically identical events — "Connection refused from 10.0.0.1"
and "Connection refused from 192.168.50.240" — will produce maximally
dissimilar embeddings.

This module solves the problem by replacing volatile tokens with
canonical placeholders *before* any embedding is computed, collapsing
the infinite cardinality of raw messages into a manageable template space.

Design principles:
  1. Every regex is compiled once at module load — O(1) lookup, zero
     recompilation on the hot path.
  2. Substitutions are applied in a deterministic, priority-ordered
     pipeline.  More-specific patterns run before less-specific ones
     to prevent partial matches from corrupting their longer siblings
     (e.g., UUID must run before bare 8-hex-digit IDs).
  3. The pipeline is exposed as a single pure function `normalize()` with
     no side effects — easy to unit-test and safe to call from multiple
     threads or coroutines simultaneously.
  4. A `NormalizationResult` dataclass bundles the cleaned template with
     the count and types of tokens that were masked, giving downstream
     consumers rich telemetry without re-running the regex.

Placeholder vocabulary:
  <TIMESTAMP>  — Any timestamp representation (ISO 8601, epoch ms/s, syslog)
  <UUID>       — RFC 4122 UUID / GUID (all variants)
  <IPV6>       — Full and compressed IPv6 addresses
  <IP>         — IPv4 addresses (dotted-quad)
  <HEX_HASH>   — Long hex strings used as SHA-1/SHA-256 digests or git SHAs
  <HEX_ID>     — Short hex identifiers (8–15 digits, e.g., process memory addr)
  <NUM_ID>     — Pure numeric identifiers that are clearly not plain counts
                  (length ≥ 5, or surrounded by context keywords like
                  user_id=, pid=, port=, etc.)
  <DURATION>   — Timing annotations like "12ms", "3.4s", "450μs"
  <SIZE>       — Memory / data-size annotations like "3.9GB", "512MB", "2KiB"
  <PORT>       — Network port numbers (1–65535) in port=<n> context
  <PATH>       — Absolute UNIX file system paths
  <URL>        — HTTP/S URLs with query strings
  <EMAIL>      — Email addresses
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final


# =============================================================================
# Compiled pattern definitions
# =============================================================================

# ---------------------------------------------------------------------------
# 1. ISO 8601 / RFC 3339 Timestamps
#    Matches: 2024-01-15T12:34:56.789Z
#              2024-01-15T12:34:56+05:30
#              2024-01-15 12:34:56,789
#              2024-01-15T12:34:56.123456+00:00
# ---------------------------------------------------------------------------
_RE_TIMESTAMP_ISO: Final = re.compile(
    r"\b\d{4}-\d{2}-\d{2}"          # Date part: YYYY-MM-DD
    r"[T ]"                           # Separator: T or space
    r"\d{2}:\d{2}:\d{2}"             # Time part: HH:MM:SS
    r"(?:[.,]\d{1,9})?"              # Optional sub-seconds
    r"(?:Z|[+-]\d{2}:?\d{2})?\b",   # Optional timezone
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 2. Syslog / Apache timestamps
#    Matches: Jan 15 12:34:56
#              15/Jan/2024:12:34:56 +0530
#    NOTE: We require the full HH:MM:SS triple to avoid false positives.
#    The month name anchor prevents collision with the IPv6 colon pattern.
# ---------------------------------------------------------------------------
_RE_TIMESTAMP_SYSLOG: Final = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"[ ]\d{1,2}[ ]\d{2}:\d{2}:\d{2}\b"
    r"|"
    r"\d{1,2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/\d{4}"
    r":\d{2}:\d{2}:\d{2}(?:\s[+-]\d{4})?",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 3. Unix epoch timestamps (ms and s precision)
#    Only matches when the number is 10–13 digits (seconds since 1970 or ms)
#    to avoid collisions with numeric IDs of similar length.
# ---------------------------------------------------------------------------
_RE_TIMESTAMP_EPOCH: Final = re.compile(
    r"(?<!\d)\d{10,13}(?!\d)"
)

# ---------------------------------------------------------------------------
# 4. RFC 4122 UUID / GUID
#    Matches: 550e8400-e29b-41d4-a716-446655440000
#              {550e8400-e29b-41d4-a716-446655440000}
# ---------------------------------------------------------------------------
_RE_UUID: Final = re.compile(
    r"\{?"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"\}?",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 5. IPv6 addresses (full and compressed notation)
#    Must run before IPv4 to avoid corrupting the :: component.
# ---------------------------------------------------------------------------
_RE_IPV6: Final = re.compile(
    r"\b(?:"
    # Full 8-group form
    r"(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}"
    r"|"
    # Compressed form with ::
    r"(?:[0-9a-f]{1,4}:)*:(?::[0-9a-f]{1,4})*"
    r"|"
    # IPv4-mapped IPv6 (::ffff:192.168.1.1)
    r"::ffff:[0-9]{1,3}(?:\.[0-9]{1,3}){3}"
    r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 6. IPv4 addresses (dotted-quad, optional CIDR suffix)
# ---------------------------------------------------------------------------
_RE_IPV4: Final = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)"
    r"(?:/\d{1,2})?\b"
)

# ---------------------------------------------------------------------------
# 7. Long hex hashes (SHA-1 = 40, SHA-256 = 64, MD5 = 32, git short ≥ 7)
#    We target strings of 32–64 hex chars as likely cryptographic digests.
#    Lower bound of 16 catches short git SHAs and short content hashes.
# ---------------------------------------------------------------------------
_RE_HEX_HASH: Final = re.compile(
    r"\b[0-9a-f]{16,64}\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 8. Short hex IDs (8–15 digits) — memory addresses, span IDs, short hashes
#    The 0x prefix is optional.
#    Negative lookbehind for underscore prevents matching the hex suffix of
#    tokens like `usr_a3f2c1b4` where the prefix carries semantic meaning.
#    We intentionally match `0x` prefixed addresses even mid-word.
# ---------------------------------------------------------------------------
_RE_HEX_ID: Final = re.compile(
    r"(?<![_A-Za-z])(?:0x)[0-9a-f]{1,15}\b"
    r"|"
    r"(?<![_A-Za-z])\b[0-9a-f]{8,15}\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 9. URLs (http/https with optional query strings and fragments)
# ---------------------------------------------------------------------------
_RE_URL: Final = re.compile(
    r"https?://[^\s\"'>)}{,;|]+"
)

# ---------------------------------------------------------------------------
# 10. Email addresses
# ---------------------------------------------------------------------------
_RE_EMAIL: Final = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

# ---------------------------------------------------------------------------
# 11. Absolute UNIX file paths (must start with /)
#     We stop before whitespace and common log-punctuation characters.
#     The pattern requires at least one non-separator character after /.
# ---------------------------------------------------------------------------
_RE_PATH: Final = re.compile(
    r"(?<![\w])(?:/[A-Za-z0-9_.\-]+)+(?:/[A-Za-z0-9_.\-]*)?"
)

# ---------------------------------------------------------------------------
# 12. Memory / data sizes  (3.9GB, 512MB, 2KiB, 1024B, 450μs treated here)
# ---------------------------------------------------------------------------
_RE_SIZE: Final = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:KiB|MiB|GiB|TiB|KB|MB|GB|TB|B)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 13. Duration annotations (12ms, 3.4s, 450us, 1.2min, 300μs)
# ---------------------------------------------------------------------------
_RE_DURATION: Final = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:μs|µs|us|ms|[smhd]|min|sec|millis|micros)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 14. Network ports in key=value context
#    Matches port=8080, :8080, port: 443
# ---------------------------------------------------------------------------
_RE_PORT: Final = re.compile(
    r"(?:port[=:\s]+)(\d{1,5})\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 15. Numeric IDs  — at least 4 digits appearing after common key prefixes
#    (user_id, pid, uid, txn, order_id, session_id, etc.) or standalone
#    long numbers (≥6 digits).  This runs LAST to avoid conflicting with
#    timestamp/size/duration patterns already substituted.
# ---------------------------------------------------------------------------
_RE_NUMERIC_KEY_VALUE: Final = re.compile(
    r"(?:"
    r"(?:user_?id|uid|pid|tid|txn_?id|order_?id|session_?id|"
    r"batch_?id|request_?id|req_?id|job_?id|task_?id|conn_?id|"
    r"attempt|count|code|line|exit|status_code|fd)[=:\s]+\d+"
    r"|"
    r"\b\d{6,}\b"          # Standalone long numerals (≥6 digits)
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 16. Percentage values used as metrics (92%, 0.5%, etc.)
#    Keep the context keyword but replace the value.
# ---------------------------------------------------------------------------
_RE_PERCENTAGE: Final = re.compile(
    r"\b\d+(?:\.\d+)?%"
)


# ---------------------------------------------------------------------------
# Helper: pre-check whether a short hex candidate is preceded by underscore
# so the HEX_ID pattern does not corrupt user-id prefixes like `usr_a3f2c1b4`.
# This guard is encoded directly in _RE_HEX_ID via the lookbehind.
# ---------------------------------------------------------------------------


# =============================================================================
# Normalisation result container
# =============================================================================


@dataclass(slots=True, frozen=True)
class NormalizationResult:
    """
    Immutable result of a single normalisation pass.

    Attributes:
        raw:           The original, unmodified log message.
        normalized:    The template string with volatile tokens replaced.
        token_counts:  Dict mapping placeholder name → replacement count.
        changed:       True if any substitution was made.
    """

    raw: str
    normalized: str
    token_counts: dict[str, int]
    changed: bool

    @property
    def total_replacements(self) -> int:
        """Sum of all token replacements made in this pass."""
        return sum(self.token_counts.values())


# =============================================================================
# Normalisation pipeline
# =============================================================================

# Ordered list of (pattern, placeholder_label) tuples.
# The order is semantically significant — see the module docstring.
_PIPELINE: Final[list[tuple[re.Pattern[str], str]]] = [
    (_RE_TIMESTAMP_ISO,       "<TIMESTAMP>"),
    (_RE_TIMESTAMP_SYSLOG,    "<TIMESTAMP>"),
    (_RE_UUID,                "<UUID>"),
    (_RE_IPV6,                "<IPV6>"),
    (_RE_IPV4,                "<IP>"),
    (_RE_URL,                 "<URL>"),
    (_RE_EMAIL,               "<EMAIL>"),
    (_RE_PATH,                "<PATH>"),
    (_RE_SIZE,                "<SIZE>"),
    (_RE_DURATION,            "<DURATION>"),
    (_RE_PORT,                "port=<PORT>"),
    (_RE_HEX_HASH,            "<HEX_HASH>"),
    (_RE_HEX_ID,              "<HEX_ID>"),
    (_RE_TIMESTAMP_EPOCH,     "<TIMESTAMP>"),
    (_RE_PERCENTAGE,          "<PCT>"),
    (_RE_NUMERIC_KEY_VALUE,   "<NUM_ID>"),
]


def normalize(raw: str) -> NormalizationResult:
    """
    Run the full normalisation pipeline on a single raw log message.

    This function is **pure** — it has no side effects and is safe to call
    concurrently from multiple threads or asyncio coroutines without any
    additional synchronisation.

    The pipeline applies each regex substitution in order, accumulating a
    count of replacements made per placeholder type.  The final output is
    the fully-normalised template string alongside rich telemetry.

    Args:
        raw: The original log message string as received from Redis.

    Returns:
        A ``NormalizationResult`` with the cleaned template and token counts.

    Example::

        >>> result = normalize(
        ...     "Failed login from 192.168.1.50 for user "
        ...     "550e8400-e29b-41d4-a716-446655440000"
        ... )
        >>> result.normalized
        'Failed login from <IP> for user <UUID>'
        >>> result.token_counts
        {'<IP>': 1, '<UUID>': 1}
    """
    text = raw
    counts: dict[str, int] = {}

    for pattern, placeholder in _PIPELINE:
        matches = pattern.findall(text)
        if matches:
            n = len(matches)
            text = pattern.sub(placeholder, text)
            counts[placeholder] = counts.get(placeholder, 0) + n

    # Collapse runs of whitespace introduced by substitutions
    text = re.sub(r" {2,}", " ", text).strip()

    return NormalizationResult(
        raw=raw,
        normalized=text,
        token_counts=counts,
        changed=text != raw,
    )


def normalize_batch(messages: list[str]) -> list[NormalizationResult]:
    """
    Normalise a list of messages and return one result per message.

    This is a thin convenience wrapper that avoids the per-call overhead
    of function lookup when processing micro-batches from the stream worker.

    Args:
        messages: List of raw log message strings.

    Returns:
        List of ``NormalizationResult`` objects in the same order.
    """
    return [normalize(msg) for msg in messages]


# =============================================================================
# Embedded verification harness
# =============================================================================
# These cases are deliberately kept inside the module so that:
#   a) They serve as always-up-to-date documentation of expected behaviour.
#   b) `python -m app.core.normalizer` prints a pass/fail report instantly.
#   c) CI can run them without a separate test framework.


_VERIFICATION_CASES: Final[list[tuple[str, str]]] = [
    # ── IPv4 & UUID ──────────────────────────────────────────────────────────
    (
        "Failed login from 192.168.1.50 for user 550e8400-e29b-41d4-a716-446655440000",
        "Failed login from <IP> for user <UUID>",
    ),
    # ── ISO 8601 timestamp ───────────────────────────────────────────────────
    (
        "Request received at 2024-01-15T12:34:56.789Z from 10.0.0.1",
        "Request received at <TIMESTAMP> from <IP>",
    ),
    # ── Syslog timestamp ──────────────────────────────────────────────────────
    (
        "Jan 15 12:34:56 kernel: OOM killer invoked",
        "<TIMESTAMP> kernel: OOM killer invoked",
    ),
    # ── Duration & size ──────────────────────────────────────────────────────
    (
        "Query executed in 12ms using 3.9GB of heap",
        "Query executed in <DURATION> using <SIZE> of heap",
    ),
    # ── URL stripping ────────────────────────────────────────────────────────
    (
        "Webhook dispatched to https://merchant.io/hooks/payment?token=abc123",
        "Webhook dispatched to <URL>",
    ),
    # ── Long hex hash ────────────────────────────────────────────────────────
    (
        "Commit a3f2c1b4d9e8f701234567890abcdef123456789 deployed to staging",
        "Commit <HEX_HASH> deployed to staging",
    ),
    # ── IPv6 ─────────────────────────────────────────────────────────────────
    (
        "Connection rejected from 2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "Connection rejected from <IPV6>",
    ),
    # ── Multiple tokens in one message ───────────────────────────────────────
    # NOTE: usr_a3f2c1b4 is NOT masked — the underscore lookbehind in
    # _RE_HEX_ID intentionally preserves prefixed identifiers like usr_*
    # because the semantic prefix is valuable context for the embedder.
    (
        "User usr_a3f2c1b4 (uuid: 6ba7b810-9dad-11d1-80b4-00c04fd430c8) "
        "logged in from 203.0.113.42 at 2024-06-01T08:00:00Z in 45ms",
        "User usr_a3f2c1b4 (uuid: <UUID>) "
        "logged in from <IP> at <TIMESTAMP> in <DURATION>",
    ),
    # ── Percentage values ─────────────────────────────────────────────────────
    (
        "Circuit breaker open | failure_rate=92%",
        "Circuit breaker open | failure_rate=<PCT>",
    ),
    # ── No volatile tokens — template should be identical to input ───────────
    (
        "Database connection pool exhausted",
        "Database connection pool exhausted",
    ),
    # ── File path ────────────────────────────────────────────────────────────
    # The path regex matches /etc/nginx/nginx.conf as a complete path token.
    # The _RE_NUMERIC_KEY_VALUE pattern then matches "line 247" as a whole
    # (the word "line" is recognised as a numeric context keyword), so the
    # final output is <PATH> <NUM_ID> — the word "line" is consumed.
    (
        "Config reload failed: invalid syntax at /etc/nginx/nginx.conf line 247",
        "Config reload failed: invalid syntax at <PATH> <NUM_ID>",
    ),
    # ── Email ─────────────────────────────────────────────────────────────────
    (
        "Password reset sent to john.doe@example.com",
        "Password reset sent to <EMAIL>",
    ),
]


def _run_verification() -> None:
    """
    Run all embedded verification cases and print a formatted report.

    Called when this module is executed directly:
        python -m app.core.normalizer
    """
    print("=" * 72)
    print("  AetherSRE Normalizer — Verification Suite")
    print("=" * 72)

    passed = 0
    failed = 0

    for i, (raw, expected) in enumerate(_VERIFICATION_CASES, start=1):
        result = normalize(raw)
        ok = result.normalized == expected

        status = "✅ PASS" if ok else "❌ FAIL"
        print(f"\n[{i:02d}] {status}")
        print(f"  Input   : {raw[:80]}")
        print(f"  Expected: {expected[:80]}")

        if not ok:
            print(f"  Got     : {result.normalized[:80]}")
            failed += 1
        else:
            passed += 1

        if result.token_counts:
            print(f"  Tokens  : {result.token_counts}")

    print()
    print("=" * 72)
    print(f"  Results: {passed} passed, {failed} failed out of {len(_VERIFICATION_CASES)} cases")
    print("=" * 72)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    _run_verification()

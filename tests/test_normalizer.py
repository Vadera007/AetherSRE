"""
AetherSRE — Day 2 Unit Tests: Normalizer
=========================================
Tests every regex substitution pattern in the normalizer module in isolation,
ensuring correct token replacement and pipeline ordering.
"""

from __future__ import annotations

import pytest

from app.core.normalizer import NormalizationResult, normalize, normalize_batch


class TestTimestampNormalization:
    """ISO 8601, syslog, and epoch timestamp masking."""

    def test_iso8601_with_utc_z(self) -> None:
        result = normalize("Request received at 2024-01-15T12:34:56.789Z")
        assert "<TIMESTAMP>" in result.normalized
        assert "2024-01-15" not in result.normalized

    def test_iso8601_with_timezone_offset(self) -> None:
        result = normalize("Event at 2024-06-01T08:00:00+05:30 processed")
        assert "<TIMESTAMP>" in result.normalized

    def test_iso8601_with_microseconds(self) -> None:
        result = normalize("Logged at 2024-01-15T12:34:56.123456+00:00")
        assert "<TIMESTAMP>" in result.normalized

    def test_syslog_timestamp(self) -> None:
        result = normalize("Jan 15 12:34:56 kernel: OOM killer invoked")
        assert "<TIMESTAMP>" in result.normalized
        assert "Jan 15" not in result.normalized

    def test_literal_text_unchanged_no_timestamp(self) -> None:
        result = normalize("Database connection pool exhausted")
        assert result.normalized == "Database connection pool exhausted"
        assert not result.changed


class TestIPNormalization:
    """IPv4 and IPv6 address masking."""

    def test_ipv4_standard(self) -> None:
        result = normalize("Failed login from 192.168.1.50")
        assert result.normalized == "Failed login from <IP>"

    def test_ipv4_with_cidr(self) -> None:
        result = normalize("Blocking subnet 10.0.0.0/8")
        assert "<IP>" in result.normalized

    def test_ipv6_full(self) -> None:
        result = normalize("Connection from 2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        assert "<IPV6>" in result.normalized

    def test_ipv4_not_confused_with_version_numbers(self) -> None:
        # "1.2.3" alone should NOT match (only 3 octets)
        result = normalize("Using library v1.2.3 for processing")
        assert "<IP>" not in result.normalized


class TestUUIDNormalization:
    """RFC 4122 UUID masking."""

    def test_standard_uuid(self) -> None:
        result = normalize(
            "Failed login for user 550e8400-e29b-41d4-a716-446655440000"
        )
        assert result.normalized == "Failed login for user <UUID>"
        assert result.token_counts.get("<UUID>", 0) == 1

    def test_uuid_in_braces(self) -> None:
        result = normalize("GUID: {6ba7b810-9dad-11d1-80b4-00c04fd430c8}")
        assert "<UUID>" in result.normalized

    def test_multiple_uuids(self) -> None:
        raw = (
            "Trace 550e8400-e29b-41d4-a716-446655440000 "
            "parent 6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        )
        result = normalize(raw)
        assert result.token_counts.get("<UUID>", 0) == 2


class TestDurationNormalization:
    """Timing annotation masking."""

    def test_milliseconds(self) -> None:
        result = normalize("Query executed in 12ms")
        assert result.normalized == "Query executed in <DURATION>"

    def test_seconds_float(self) -> None:
        result = normalize("Request latency 3.4s exceeds SLA")
        assert "<DURATION>" in result.normalized

    def test_microseconds_symbol(self) -> None:
        result = normalize("Cache lookup 450μs")
        assert "<DURATION>" in result.normalized


class TestSizeNormalization:
    """Memory and data-size annotation masking."""

    def test_gigabytes(self) -> None:
        result = normalize("Heap OOM: used 3.9GB of 4GB")
        assert "<SIZE>" in result.normalized

    def test_megabytes(self) -> None:
        result = normalize("Allocated 512MB for JVM")
        assert "<SIZE>" in result.normalized

    def test_size_and_duration_combined(self) -> None:
        result = normalize("Query executed in 12ms using 3.9GB of heap")
        assert "<DURATION>" in result.normalized
        assert "<SIZE>" in result.normalized


class TestURLNormalization:
    """URL masking."""

    def test_https_url(self) -> None:
        result = normalize("Webhook dispatched to https://merchant.io/hooks/payment?token=abc123")
        assert result.normalized == "Webhook dispatched to <URL>"

    def test_http_url(self) -> None:
        result = normalize("Health check http://10.0.0.1:8080/health returned 200")
        assert "<URL>" in result.normalized


class TestEmailNormalization:
    """Email address masking."""

    def test_standard_email(self) -> None:
        result = normalize("Password reset sent to john.doe@example.com")
        assert result.normalized == "Password reset sent to <EMAIL>"


class TestHexHashNormalization:
    """Hex hash and short hex ID masking."""

    def test_long_hex_hash(self) -> None:
        result = normalize(
            "Commit a3f2c1b4d9e8f701234567890abcdef123456789 deployed to staging"
        )
        assert "<HEX_HASH>" in result.normalized

    def test_short_hex_id_is_masked(self) -> None:
        result = normalize("Memory address 0x7ffee4b2c3d0 faulted")
        assert "0x7ffee4b2c3d0" not in result.normalized


class TestPercentageNormalization:
    """Percentage metric masking."""

    def test_integer_percentage(self) -> None:
        result = normalize("Circuit breaker open | failure_rate=92%")
        assert "<PCT>" in result.normalized
        assert "92%" not in result.normalized


class TestCompositeNormalization:
    """Multi-token messages exercising multiple pipeline stages."""

    def test_canonical_ip_and_uuid_case(self) -> None:
        result = normalize(
            "Failed login from 192.168.1.50 for user "
            "550e8400-e29b-41d4-a716-446655440000"
        )
        assert result.normalized == "Failed login from <IP> for user <UUID>"

    def test_timestamp_and_ip(self) -> None:
        result = normalize("Request received at 2024-01-15T12:34:56.789Z from 10.0.0.1")
        assert "<TIMESTAMP>" in result.normalized
        assert "<IP>" in result.normalized

    def test_unchanged_message_has_zero_replacements(self) -> None:
        result = normalize("Database connection pool exhausted")
        assert result.total_replacements == 0
        assert not result.changed

    def test_result_is_immutable(self) -> None:
        result = normalize("Login from 10.0.0.1")
        # NormalizationResult is frozen; attribute assignment must raise
        with pytest.raises((AttributeError, TypeError)):
            result.normalized = "mutated"  # type: ignore[misc]


class TestNormalizeBatch:
    """Batch normalisation API."""

    def test_batch_preserves_order(self) -> None:
        messages = [
            "Login from 192.168.1.1",
            "Database connection pool exhausted",
            "UUID 550e8400-e29b-41d4-a716-446655440000 processed",
        ]
        results = normalize_batch(messages)
        assert len(results) == 3
        assert "<IP>" in results[0].normalized
        assert results[1].normalized == "Database connection pool exhausted"
        assert "<UUID>" in results[2].normalized

    def test_empty_batch(self) -> None:
        results = normalize_batch([])
        assert results == []

    def test_single_item_batch(self) -> None:
        results = normalize_batch(["Payment from 10.0.0.50"])
        assert len(results) == 1
        assert "<IP>" in results[0].normalized

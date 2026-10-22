"""Shared storage codecs and physical identity guards migrated from the old suite."""

from datetime import date, datetime, time, timedelta, timezone
import json
import unittest

from neo4j.spatial import CartesianPoint, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time

from app.infrastructure.graph_storage.subgraph_records import (
    SubgraphError, decode, digest, encode, identifier, json_view, validate_snapshot)
from app.infrastructure.risk_map.neo4j_bridge import copy_plan
from app.infrastructure.risk_map.test.test_copy_first import source_graph


class RecordCodecTests(unittest.TestCase):
    def test_supported_types_roundtrip(self):
        values = [None, "Date", False, True, 0, -2, 1.5, float("nan"), float("inf"), b"\x00\xff",
                  bytearray(b"x"), Date(2026, 10, 16), DateTime(2026, 10, 16, 3, 4, 5.123456789),
                  Time(3, 4, 5.123456789), Duration(months=1, days=2, seconds=3, nanoseconds=4),
                  CartesianPoint((1, 2)), WGS84Point((120, 30, 7)),
                  datetime(2026, 10, 16, tzinfo=timezone.utc), date(2026, 10, 16),
                  time(3, 4, 5), timedelta(days=3), [1, 2], [], {"t": "Date", "v": "literal"}]
        for value in values:
            with self.subTest(kind=type(value).__name__):
                self.assertEqual(digest(value), digest(decode(encode(value))))
                json.dumps(json_view(value), allow_nan=False)

    def test_timezone_region_preserved(self):
        import pytz
        value = DateTime.from_native(pytz.timezone("Asia/Shanghai").localize(datetime(2026, 10, 16)))
        self.assertEqual(digest(value), digest(decode(encode(value))))

    def test_unsupported_types_rejected(self):
        for value in (object(), {1: "invalid-key"}):
            with self.assertRaises(SubgraphError):
                encode(value)

    def test_identifier_escape_and_invalid_input(self):
        self.assertEqual(identifier("中国`法规"), "`中国``法规`")
        for value in (None, "", " ", "a\nb", "\\u0060bad"):
            with self.assertRaises(SubgraphError):
                identifier(value)

    def test_physical_duplicate_missing_endpoint_rejected(self):
        snapshot = source_graph(1)
        snapshot["nodes"].append(snapshot["nodes"][0])
        with self.assertRaises(SubgraphError):
            validate_snapshot(snapshot)
        snapshot = source_graph(1)
        snapshot["edges"][0]["p"]["endNodeElementId"] = "missing"
        with self.assertRaises(SubgraphError):
            validate_snapshot(snapshot)

    def test_conflicting_source_scope_not_normalized_away(self):
        snapshot = source_graph(1)
        snapshot["nodes"][0]["n"]["properties"]["graph_tag"] = "other"
        with self.assertRaises(ValueError):
            copy_plan(snapshot, "source", "target")


if __name__ == "__main__":
    unittest.main()

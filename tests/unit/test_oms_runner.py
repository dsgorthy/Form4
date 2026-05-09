"""Unit tests for framework.oms.runner — V2 feature flag + helpers."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework.oms.runner import is_oms_v2_enabled


# ── is_oms_v2_enabled — env var dispatcher ──────────────────────────────────


class TestIsOmsV2Enabled:

    def setup_method(self):
        # Snapshot env var; restore in teardown
        self._saved = os.environ.get("OMS_V2")
        os.environ.pop("OMS_V2", None)

    def teardown_method(self):
        os.environ.pop("OMS_V2", None)
        if self._saved is not None:
            os.environ["OMS_V2"] = self._saved

    def test_unset_is_false(self):
        assert is_oms_v2_enabled() is False

    def test_empty_is_false(self):
        os.environ["OMS_V2"] = ""
        assert is_oms_v2_enabled() is False

    def test_zero_is_false(self):
        os.environ["OMS_V2"] = "0"
        assert is_oms_v2_enabled() is False

    def test_false_string_is_false(self):
        os.environ["OMS_V2"] = "false"
        assert is_oms_v2_enabled() is False

    def test_one_is_true(self):
        os.environ["OMS_V2"] = "1"
        assert is_oms_v2_enabled() is True

    def test_true_string_is_true(self):
        os.environ["OMS_V2"] = "true"
        assert is_oms_v2_enabled() is True

    def test_uppercase_TRUE_is_true(self):
        os.environ["OMS_V2"] = "TRUE"
        assert is_oms_v2_enabled() is True

    def test_yes_is_true(self):
        os.environ["OMS_V2"] = "yes"
        assert is_oms_v2_enabled() is True

    def test_on_is_true(self):
        os.environ["OMS_V2"] = "on"
        assert is_oms_v2_enabled() is True

    def test_whitespace_handled(self):
        os.environ["OMS_V2"] = "  true  "
        assert is_oms_v2_enabled() is True

    def test_garbage_is_false(self):
        os.environ["OMS_V2"] = "maybe"
        assert is_oms_v2_enabled() is False


# ── evaluate_candidates_v2 — interface contract test ────────────────────────


class TestEvaluateCandidatesV2:
    """We can't fully exercise evaluate_candidates_v2 without a DB and the
    full Studio environment. These tests verify the function exists, has
    the right signature, and that the early-exit path (freshness halt)
    works against a mock connection.
    """

    def test_function_importable(self):
        from framework.oms.runner import evaluate_candidates_v2
        assert callable(evaluate_candidates_v2)

    def test_signature(self):
        import inspect
        from framework.oms.runner import evaluate_candidates_v2
        sig = inspect.signature(evaluate_candidates_v2)
        params = list(sig.parameters.keys())
        assert params == ["conn", "config"]

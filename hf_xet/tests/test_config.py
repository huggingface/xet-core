"""
Tests for XetConfig: construction, field access, update, and type coercions.
"""

from datetime import timedelta

import pytest

import hf_xet


class TestXetConfig:
    def test_keys_items_len_and_iteration_are_consistent(self):
        config = hf_xet.XetConfig()
        keys = config.keys()
        assert len(config) == len(keys)
        assert frozenset(keys) == frozenset(k for k, _ in config.items())
        assert sum(1 for _ in config) == len(keys)
        for k, _ in config:
            assert isinstance(k, str)

    def test_get_and_getitem_agree_for_valid_paths(self):
        config = hf_xet.XetConfig()
        assert config.get("data.max_concurrent_file_ingestion") == config[
            "data.max_concurrent_file_ingestion"
        ]
        assert config.get("data.progress_update_interval") == config[
            "data.progress_update_interval"
        ]

    def test_with_config_dict_sets_distinct_field_types(self):
        # Duration accepts either a humantime string ("501ms") or a timedelta.
        cfg = hf_xet.XetConfig().with_config(
            {
                "data.max_concurrent_file_ingestion": 4,
                "data.progress_update_interval": "501ms",
                "data.local_cas_scheme": "local://test-scheme/",
            }
        )
        assert cfg.get("data.max_concurrent_file_ingestion") == 4
        assert cfg.get("data.progress_update_interval") == timedelta(milliseconds=501)
        assert cfg.get("data.local_cas_scheme") == "local://test-scheme/"

    def test_with_config_duration_accepts_timedelta(self):
        cfg = hf_xet.XetConfig().with_config("data.progress_update_interval", timedelta(milliseconds=501))
        assert cfg.get("data.progress_update_interval") == timedelta(milliseconds=501)

    def test_with_config_is_immutable_on_original(self):
        base = hf_xet.XetConfig()
        before = base.get("data.max_concurrent_file_ingestion")
        updated = base.with_config("data.max_concurrent_file_ingestion", 999)
        assert base.get("data.max_concurrent_file_ingestion") == before
        assert updated.get("data.max_concurrent_file_ingestion") == 999

    def test_with_config_rejects_invalid_arguments(self):
        with pytest.raises(TypeError, match="second argument"):
            hf_xet.XetConfig().with_config(
                {"data.max_concurrent_file_ingestion": 1},
                "extra",
            )
        with pytest.raises(TypeError, match="value argument"):
            hf_xet.XetConfig().with_config("data.max_concurrent_file_ingestion")

    def test_get_raises_value_error_for_invalid_paths(self):
        with pytest.raises(ValueError):
            hf_xet.XetConfig().get("no_dot_segment")
        with pytest.raises(ValueError):
            hf_xet.XetConfig().get("not_a_real_config_group.field")

    def test_getitem_raises_key_error_for_unknown_path(self):
        missing = "definitely_missing.unlikely_xyz_field_quack"
        with pytest.raises(KeyError) as ei:
            hf_xet.XetConfig()[missing]
        assert ei.value.args[0] == missing

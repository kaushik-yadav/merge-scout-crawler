import os

import pytest

from mergescout.config import load_config, load_dotenv
from mergescout.timeutil import parse_time


def test_reads_token_and_data_dir_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("GITHUB_TOKEN", "abc")
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path / "d"))
    cfg = load_config(env_file=None)
    assert cfg.github_token == "abc"
    assert cfg.data_dir == tmp_path / "d"
    assert cfg.data_dir.is_dir()


def test_missing_token_gives_clear_error(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    cfg = load_config(env_file=None)
    with pytest.raises(RuntimeError, match="GITHUB_TOKEN is not set"):
        cfg.require_token()


def test_dotenv_fills_missing_values_only(monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text("GITHUB_TOKEN=\"from_file\"\n# a comment\nMS_X=1\nnot a pair\n", encoding="utf-8")
    monkeypatch.setattr(os, "environ", {"GITHUB_TOKEN": "from_env"})
    load_dotenv(env)
    assert os.environ["GITHUB_TOKEN"] == "from_env"
    assert os.environ["MS_X"] == "1"


def test_parse_time():
    t = parse_time("2026-10-01T15:00:01Z")
    assert (t.year, t.hour, t.second, t.utcoffset().total_seconds()) == (2026, 15, 1, 0)
    assert parse_time(None) is None
    assert parse_time("not a time") is None

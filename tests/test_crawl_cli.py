import pyarrow.parquet as pq

from mergescout.crawl import __main__ as cli
from mergescout.crawl.__main__ import main
from mergescout.crawl.state import CrawlState
from test_crawl_runner import GQL, budget
from test_crawl_seed import repo, write_basics


def env(monkeypatch, tmp_path, token="t"):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    if token:
        monkeypatch.setenv("GITHUB_TOKEN", token)
    else:
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)


def test_status_on_empty_state(monkeypatch, tmp_path, capsys):
    env(monkeypatch, tmp_path)
    assert main(["status"]) == 0
    assert "repos={}" in capsys.readouterr().out


def test_run_without_token_fails_clearly(monkeypatch, tmp_path, capsys):
    env(monkeypatch, tmp_path, token=None)
    assert main(["run", "--minutes", "1"]) == 1
    assert "GITHUB_TOKEN" in capsys.readouterr().err


def test_backup_copies_state(monkeypatch, tmp_path):
    env(monkeypatch, tmp_path)
    CrawlState(tmp_path / "crawl.sqlite").upsert_repos([{"repo_id": 1, "name": "o/r", "stars": 100,
                                                          "pushed_at": None, "priority": 0}])
    assert main(["backup", str(tmp_path / "copy.sqlite")]) == 0
    assert CrawlState(tmp_path / "copy.sqlite").counts() == {"queued": 1}


def test_seed_then_run_end_to_end(monkeypatch, tmp_path, capsys):
    from datetime import datetime, timezone
    env(monkeypatch, tmp_path, token=None)
    write_basics(tmp_path, [repo(1, "o/a", 500, pushed=datetime.now(timezone.utc)),
                            repo(2, "o/b", 300, pushed=datetime.now(timezone.utc), opp=2)])
    assert main(["seed"]) == 0
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("MS_RUN_ID", "42-1")
    b = budget()
    made = []
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: made.append(kw) or GQL(b))
    assert main(["run", "--minutes", "1", "--in-flight", "2"]) == 0
    assert made and callable(made[0]["max_wait"]) and 100 < made[0]["max_wait"]() <= 120
    assert all(kw["gate"] is made[0]["gate"] for kw in made) and made[0]["gate"] is not None
    assert pq.read_table(tmp_path / "crawl" / "prs" / "run=42-1-000.parquet").num_rows == 2
    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "'done': 2" in out and "42-1" in out


def test_run_exits_nonzero_when_everything_failed(monkeypatch, tmp_path):
    env(monkeypatch, tmp_path)
    CrawlState(tmp_path / "crawl.sqlite").upsert_repos([{"repo_id": 1, "name": "o/r1", "stars": 100,
                                                          "pushed_at": None, "priority": 0}])
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: GQL(budget(), fail_for={"o/r1"}))
    assert main(["run", "--minutes", "1"]) == 1


def test_run_uses_every_identity_it_is_given(monkeypatch, tmp_path, capsys):
    env(monkeypatch, tmp_path)
    monkeypatch.setenv("MS_APP_TOKEN", "app")
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "act")
    CrawlState(tmp_path / "crawl.sqlite").upsert_repos([{"repo_id": i, "name": f"o/r{i}", "stars": 100,
                                                          "pushed_at": None, "priority": 0} for i in range(1, 30)])
    tokens = []
    monkeypatch.setattr(cli, "GitHubGraphQL", lambda token, **kw: tokens.append(token) or GQL(budget()))
    assert main(["run", "--minutes", "1", "--in-flight", "5"]) == 0
    out = capsys.readouterr().out
    assert "github personal:" in out and "github app:" in out and "github actions:" in out
    assert set(tokens) == {"t", "app", "act"}

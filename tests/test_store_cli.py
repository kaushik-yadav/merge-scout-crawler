from mergescout.store import __main__ as cli
from test_store import MemReleases, make_data


def test_missing_token_or_repo_fails_clearly(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("MS_RELEASE_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    assert cli.main(["push"]) == 1
    assert "MS_RELEASE_TOKEN" in capsys.readouterr().err


def test_push_then_pull_through_the_command_line(monkeypatch, tmp_path):
    monkeypatch.setenv("MS_RELEASE_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/repo")
    rel = MemReleases()
    made = []
    monkeypatch.setattr(cli, "Releases", lambda token, repo: made.append((token, repo)) or rel)
    src, dst = tmp_path / "src", tmp_path / "dst"
    make_data(src)
    monkeypatch.setenv("MS_DATA_DIR", str(src))
    assert cli.main(["push", "--with-discover"]) == 0
    monkeypatch.setenv("MS_DATA_DIR", str(dst))
    assert cli.main(["pull"]) == 0
    assert (dst / "crawl.sqlite").exists() and made[0] == ("t", "me/repo")


def test_store_repo_setting_overrides_the_workflow_repo(monkeypatch, tmp_path):
    monkeypatch.setenv("MS_RELEASE_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/crawler-repo")
    monkeypatch.setenv("MS_STORE_REPO", "me/private-project")
    monkeypatch.setenv("MS_DATA_DIR", str(tmp_path))
    made = []
    monkeypatch.setattr(cli, "Releases", lambda token, repo: made.append(repo) or MemReleases())
    assert cli.main(["pull"]) == 0
    assert made == ["me/private-project"]

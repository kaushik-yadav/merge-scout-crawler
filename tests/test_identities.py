from mergescout.github.identities import identities_from_env, slots


def test_identities_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("MS_APP_TOKEN", "app")
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "act")
    made = []
    ids = identities_from_env("pat", 120, client=lambda token, **kw: made.append((token, kw)) or token)
    assert [i.name for i in ids] == ["personal", "app", "actions"] and [i.share for i in ids] == [1.0, 1.0, 0.2]
    assert [i.make_gql() for i in ids] == ["pat", "app", "act"]
    assert len({id(kw["gate"]) for _, kw in made}) == 3 and all(kw["max_wait"] == 120 for _, kw in made)


def test_missing_or_repeated_tokens_are_skipped(monkeypatch):
    monkeypatch.delenv("MS_APP_TOKEN", raising=False)
    monkeypatch.setenv("MS_ACTIONS_TOKEN", "pat")
    assert [i.name for i in identities_from_env("pat", None, client=lambda t, **kw: t)] == ["personal"]


def test_slots_follow_the_budget_share():
    assert (slots(12, 1.0), slots(12, 0.2), slots(2, 0.2)) == (12, 2, 1)

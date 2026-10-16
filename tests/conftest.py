import gzip
import os

import orjson
import pytest

# Tests create many throwaway git repos; a system-wide core.fsmonitor would leave a watcher process for each.
os.environ.update({"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.fsmonitor", "GIT_CONFIG_VALUE_0": "false"})


def pytest_collection_modifyitems(config, items):
    # Network tests only run when asked, so the normal suite stays offline.
    if os.environ.get("MS_NETWORK_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="network test; set MS_NETWORK_TESTS=1 to run")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def make_gz():
    def _make(events, extra_lines=()):
        lines = [orjson.dumps(e) for e in events] + [line.encode() for line in extra_lines]
        return gzip.compress(b"\n".join(lines) + b"\n")
    return _make


class FakeGQL:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []
        self.kwargs = []

    def query(self, query, variables=None, **kw):
        from mergescout.github.graphql import GraphQLResult
        variables = variables or {}
        self.calls.append((query, variables))
        self.kwargs.append(kw)
        data, cost = self.responder(query, variables)
        return GraphQLResult(data=data, cost=cost, remaining=4000, reset_at="2026-10-11T00:00:00Z")


@pytest.fixture
def fake_gql():
    return FakeGQL

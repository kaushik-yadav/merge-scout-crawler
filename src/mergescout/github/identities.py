import os
from collections.abc import Callable
from dataclasses import dataclass

from mergescout.github.graphql import Gate, GitHubGraphQL

# Hourly points relative to a personal token: a GitHub App also gets 5,000, the workflow token 1,000.
SHARES = {"personal": 1.0, "app": 1.0, "actions": 0.2}


@dataclass
class Identity:
    name: str
    make_gql: Callable
    gate: Gate
    share: float = 1.0


def slots(in_flight: int, share: float) -> int:
    return max(1, round(in_flight * share))


def identities_from_env(primary_token: str, max_wait: float | None, client=GitHubGraphQL) -> list[Identity]:
    # Each identity has its own limits, so each gets its own shared wait; the same token is never used twice.
    found = [("personal", primary_token), ("app", os.environ.get("MS_APP_TOKEN")),
             ("actions", os.environ.get("MS_ACTIONS_TOKEN"))]
    out, seen = [], set()
    for name, token in found:
        if not token or token in seen:
            continue
        seen.add(token)
        gate = Gate()
        out.append(Identity(name, lambda t=token, g=gate: client(t, max_wait=max_wait, gate=g), gate, SHARES[name]))
    return out

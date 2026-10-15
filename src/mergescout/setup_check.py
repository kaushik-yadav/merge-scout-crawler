"""Check the crawler repo's tokens without crawling: python -m mergescout.setup_check"""
import os
import sys
import tempfile
from pathlib import Path

from mergescout.config import load_config
from mergescout.github.graphql import GitHubGraphQL
from mergescout.github.identities import Identity, identities_from_env
from mergescout.github.releases import Releases

PROBE = 'query { rateLimit { cost remaining limit resetAt } repository(owner: "psf", name: "requests") { stargazerCount } }'


def check_identity(identity: Identity) -> tuple[bool, str]:
    try:
        data = identity.make_gql().query(PROBE).data
    except Exception as exc:
        return False, str(exc)
    rate = data.get("rateLimit") or {}
    if not (data.get("repository") or {}).get("stargazerCount"):
        return False, "token works but cannot read public repositories"
    return True, f"{rate.get('limit')} points/hour, {rate.get('remaining')} left"


def check_store(releases, tag: str = "crawl-state") -> tuple[bool, str]:
    # Proves read and write with one tiny file that is removed straight away.
    try:
        if releases.get(tag) is None:
            return False, f"can reach the repo but release '{tag}' is missing"
        with tempfile.TemporaryDirectory() as tmp:
            probe = Path(tmp) / "setup-check.txt"
            probe.write_text("setup check, safe to delete\n", encoding="utf-8")
            asset = releases.upload(tag, probe)
        releases.delete_asset(asset)
    except Exception as exc:
        return False, str(exc)
    return True, "read and write both work"


def main() -> int:
    cfg = load_config()
    results = []
    if not cfg.github_token:
        results.append(("personal token", False, "GITHUB_TOKEN is empty (secret MS_GITHUB_TOKEN)"))
    else:
        for identity in identities_from_env(cfg.github_token, max_wait=60, client=GitHubGraphQL):
            results.append((f"{identity.name} token", *check_identity(identity)))
        names = {i.name for i in identities_from_env(cfg.github_token, None)}
        for expected in ("app", "actions"):
            if expected not in names:
                results.append((f"{expected} token", False, "not passed to this run"))
    token, repo = os.environ.get("MS_RELEASE_TOKEN"), os.environ.get("MS_STORE_REPO")
    if not token or not repo:
        results.append(("data store", False, "MS_RELEASE_TOKEN or MS_STORE_REPO is empty (secret MS_STORE_TOKEN)"))
    else:
        results.append((f"data store ({repo})", *check_store(Releases(token, repo))))
    for name, ok, detail in results:
        print(f"{'OK  ' if ok else 'FAIL'} {name}: {detail}", flush=True)
    return 0 if all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())

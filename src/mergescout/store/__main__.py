import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from mergescout.config import load_config
from mergescout.github.releases import Releases
from mergescout.store.sync import pull, push


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m mergescout.store",
                                     description="Keep crawl data and state in this repo's GitHub releases.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    ps = sub.add_parser("push", help="Upload new crawl batches and a state backup, then clear uploaded batches")
    ps.add_argument("--with-discover", action="store_true", help="Also back up the sweep results")
    sub.add_parser("pull", help="Restore state from the releases when the local copy is missing")
    put = sub.add_parser("put", help="Upload one file to a release tag")
    put.add_argument("path")
    put.add_argument("--tag", required=True)
    # MS_STORE_REPO lets a crawler repo keep its data in another (private) repo; otherwise the running repo.
    parser.add_argument("--repo", default=os.environ.get("MS_STORE_REPO") or os.environ.get("GITHUB_REPOSITORY"),
                        help="owner/name of the repo whose releases hold the data")
    args = parser.parse_args(argv)

    token = os.environ.get("MS_RELEASE_TOKEN")
    if not token or not args.repo:
        print("Set MS_RELEASE_TOKEN (a token that can write releases) and GITHUB_REPOSITORY or --repo.",
              file=sys.stderr)
        return 1
    cfg = load_config()
    releases = Releases(token, args.repo)
    log = lambda msg: print(msg, flush=True)  # noqa: E731
    if args.cmd == "push":
        push(cfg.data_dir, releases, datetime.now(timezone.utc), with_discover=args.with_discover, log=log)
    elif args.cmd == "put":
        releases.upload(args.tag, Path(args.path))
        log(f"uploaded {Path(args.path).name} to {args.tag}")
    else:
        pull(cfg.data_dir, releases, log=log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

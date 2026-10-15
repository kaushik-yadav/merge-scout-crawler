import gzip
import os
import re
import shutil
import sqlite3
import tarfile
from datetime import datetime
from pathlib import Path

STATE_TAG = "crawl-state"


def data_tag(now: datetime) -> str:
    # One data release per ISO week: hourly runs stay well under GitHub's 1,000 files per release.
    return f"crawl-{now:%G-W%V}"
RUN_FILE = re.compile(r"^run=(?P<run>.+)-(?P<seq>\d{3})\.parquet$")


def pending_runs(data_dir: Path) -> dict[str, list[Path]]:
    runs: dict[str, list[Path]] = {}
    for p in sorted((data_dir / "crawl").glob("*/run=*.parquet")):
        m = RUN_FILE.match(p.name)
        if m:
            runs.setdefault(m["run"], []).append(p)
    return runs


def _tar(paths: list[Path], base: Path, out: Path) -> Path:
    with tarfile.open(out, "w") as tar:
        for p in paths:
            tar.add(p, arcname=p.relative_to(base).as_posix())
    return out


def safe_extract(archive: Path, dest: Path) -> None:
    # Only plain files and folders that land inside dest; anything else means a broken or hostile archive.
    root = dest.resolve()
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        for m in members:
            target = (dest / m.name).resolve()
            if not (m.isfile() or m.isdir()) or (target != root and root not in target.parents):
                raise ValueError(f"refusing archive entry {m.name!r}")
        dest.mkdir(parents=True, exist_ok=True)
        for m in members:
            target = dest / m.name
            if m.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(m) as src, target.open("wb") as out:
                shutil.copyfileobj(src, out)


def push(data_dir: Path, releases, now: datetime, with_discover: bool = False, log=print) -> dict:
    # Upload first, delete after: a failed upload leaves the files for the next run to retry.
    outbox = data_dir / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    out = {"runs": 0, "state": False, "discover": False}
    for run, files in pending_runs(data_dir).items():
        bundle = _tar(files, data_dir, outbox / f"crawl-{run}.tar")
        releases.upload(data_tag(now), bundle)
        for p in files:
            p.unlink()
        bundle.unlink()
        out["runs"] += 1
    state_file = data_dir / "crawl.sqlite"
    if state_file.exists():
        snap, packed = outbox / "crawl.sqlite", outbox / "crawl.sqlite.gz"
        src, dst = sqlite3.connect(state_file), sqlite3.connect(snap)
        with dst:
            src.backup(dst)
        src.close()
        dst.close()
        with snap.open("rb") as f, gzip.open(packed, "wb") as g:
            shutil.copyfileobj(f, g)
        releases.upload(STATE_TAG, packed)
        snap.unlink()
        packed.unlink()
        out["state"] = True
    discover = data_dir / "discover"
    if with_discover and discover.exists():
        files = [p for p in sorted(discover.rglob("*")) if p.is_file()]
        bundle = _tar(files, data_dir, outbox / "discover.tar")
        releases.upload(STATE_TAG, bundle)
        bundle.unlink()
        out["discover"] = True
    log(f"pushed: {out}")
    return out


def pull(data_dir: Path, releases, log=print) -> dict:
    # Only used when the Actions cache is gone; local files always win.
    outbox = data_dir / "outbox"
    out = {"state": False, "discover": False}
    state_file = data_dir / "crawl.sqlite"
    if not state_file.exists():
        packed = outbox / "crawl.sqlite.gz"
        if releases.download(STATE_TAG, "crawl.sqlite.gz", packed):
            part = state_file.with_name(state_file.name + ".part")
            with gzip.open(packed, "rb") as g, part.open("wb") as f:
                shutil.copyfileobj(g, f)
            os.replace(part, state_file)
            packed.unlink()
            out["state"] = True
    if not (data_dir / "discover").exists():
        bundle = outbox / "discover.tar"
        if releases.download(STATE_TAG, "discover.tar", bundle):
            safe_extract(bundle, data_dir)
            bundle.unlink()
            out["discover"] = True
    log(f"pulled: {out}")
    return out

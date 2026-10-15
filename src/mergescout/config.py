import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(path: Path) -> None:
    # Read KEY=VALUE lines into the environment without overriding values already set.
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"").strip("'"))


@dataclass(frozen=True)
class Config:
    github_token: str | None
    data_dir: Path

    def require_token(self) -> str:
        if not self.github_token:
            raise RuntimeError("GITHUB_TOKEN is not set. Add it to .env (see .env.example).")
        return self.github_token


def load_config(env_file: Path | None = Path(".env")) -> Config:
    if env_file is not None:
        load_dotenv(env_file)
    data_dir = Path(os.environ.get("MS_DATA_DIR") or Path(tempfile.gettempdir()) / "mergescout")
    data_dir.mkdir(parents=True, exist_ok=True)
    return Config(github_token=os.environ.get("GITHUB_TOKEN") or None, data_dir=data_dir)

# merge-scout-crawler

Data collector for MergeScout. It finds active public GitHub repositories with 100+ stars and records, for each one, the last year of pull requests, open issues with recent comments, repository settings and contributing rules, and what happens to each issue over time (claimed, PR opened, merged, closed and why).

It runs on GitHub Actions:

- **Discover** (daily): lists active repositories and queues them, the ones with most beginner-friendly issues per expected cost first.
- **Crawl** (hourly): crawls the next repositories in the queue until the hour's GitHub API points run out; later visits only fetch what changed.
- **Keep schedules alive** (monthly): a small commit so GitHub keeps the schedules on.

Only public data is read, with one personal token, and GitHub's rate limits are respected (one shared wait when GitHub asks to slow down).

Local run: `pip install -e ".[dev]"`, put `GITHUB_TOKEN=...` in `.env`, then `python -m mergescout.crawl run --minutes 5 --limit 20`.

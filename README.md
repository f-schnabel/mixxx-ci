# mixxxdj CI queue

Watches the GitHub Actions queue of the [mixxxdj](https://github.com/mixxxdj) organization, live at
https://mixxx-ci.schnabel.dev. The org shares 60 concurrent jobs and only a few macOS runners, so macOS jobs
regularly wait for hours.

`monitor.py` polls the GitHub API every minute and serves:

- `/`: a live page with the waiting and running jobs, marking jobs of outdated pull request runs as
  superseded, and a chart of the queue over the last 6 hours to 14 days
- `/api/state`: the current jobs as JSON
- `/api/history?hours=24`: the chart data

Runs and jobs are cached in `data/cache.sqlite`. The jobs of unfinished runs are fetched on every poll; a
finished run's jobs are only fetched again when the run's `updated_at` changes, so finished runs cost one
request. The chart is computed from the jobs' created,
started and completed times. On the first start a background thread fills the cache with the runs of the last
14 days, keeping 1000 requests per hour for the live polling.

It only needs Python 3.10+ and no packages.

## Run locally

```sh
python monitor.py          # http://127.0.0.1:8765, uses `gh auth token`
python monitor.py --once   # poll once, print a summary and exit
```

Set `GITHUB_TOKEN` to use a token instead of the GitHub CLI. `--days` sets how much history is kept.

## Deploy on the VPS

Every push to `main` is checked and then deployed by `.github/workflows/deploy.yml`: it connects to the VPS
over SSH, clones or pulls the repo into `~/mixxx-ci` and runs `deploy/install.sh`. That script sets up the
`mixxx-ci-queue` user service and links `deploy/mixxx-ci.caddyfile` into `/etc/caddy`.

Setup, once:

1. Add the repository secrets `OCI_HOST` and `OCI_PRIVATE_KEY` (SSH key of the `ubuntu` user), then run the
   workflow.
2. Create a [fine-grained token](https://github.com/settings/personal-access-tokens/new) with public
   repository access and no permissions; it only raises the rate limit to 5000 requests per hour. Put it into
   `~/mixxx-ci/.env` on the VPS (the first deploy creates the file) and run
   `systemctl --user restart mixxx-ci-queue`.

```sh
systemctl --user status mixxx-ci-queue
journalctl --user -u mixxx-ci-queue -f
```

# mixxxdj CI queue

Watches the GitHub Actions queue of the [mixxxdj](https://github.com/mixxxdj) organization. The org shares
60 concurrent jobs, at most 5 of them on macOS, so macOS jobs regularly wait for hours.

`monitor.py` polls the GitHub API every minute and serves:

- `/`: a live page with the waiting and running jobs, marking jobs of outdated pull request runs as superseded
- `/api/state`: the same data as JSON
- `/metrics`: Prometheus metrics for the Grafana dashboard in `deploy/grafana`

It only needs Python 3.10+ and no packages.

## Run locally

```sh
python monitor.py          # http://127.0.0.1:8765, uses `gh auth token`
python monitor.py --once   # print a summary and exit
```

Set `GITHUB_TOKEN` to use a token instead of the GitHub CLI. Chart history is kept in `history.jsonl`.

## Deploy on the VPS

Every push to `main` is checked and then deployed by `.github/workflows/deploy.yml`: it connects to the VPS
over SSH, clones or pulls the repo into `~/mixxx-ci` and runs `deploy/install.sh`. That script sets up the
`mixxx-ci-queue` user service, links `deploy/mixxx-ci.caddyfile` into `/etc/caddy` (serving
https://mixxx-ci.schnabel.dev) and provisions the dashboard into the Grafana folder "Mixxx".

Setup, once:

1. Add the repository secrets `OCI_HOST` and `OCI_PRIVATE_KEY` (SSH key of the `ubuntu` user), then run the
   workflow.
2. Create a [fine-grained token](https://github.com/settings/personal-access-tokens/new) with public
   repository access and no permissions. It only raises the rate limit; the monitor uses about 2500 of the
   5000 requests per hour. Put it into `~/mixxx-ci/.env` on the VPS (the first deploy creates the file) and
   run `systemctl --user restart mixxx-ci-queue`.
3. Add the scrape job from `deploy/prometheus-scrape.yml` to `prometheus.yml` and restart Prometheus.

```sh
systemctl --user status mixxx-ci-queue
journalctl --user -u mixxx-ci-queue -f
curl -s localhost:8765/metrics | head
```

## Metrics

| Metric | Labels |
| --- | --- |
| `mixxx_ci_jobs` | `runner` (macOS, Linux, Windows, Other), `status` (running, queued) |
| `mixxx_ci_jobs_by_label` | `repo`, `label`, `status` |
| `mixxx_ci_superseded_jobs` | `runner`, `status` |
| `mixxx_ci_oldest_queued_seconds` | `runner` |
| `mixxx_ci_my_jobs` | `status`: jobs triggered by the token owner |
| `mixxx_ci_runner_limit` | `runner` (all, macOS) |
| `mixxx_ci_github_rate_remaining`, `mixxx_ci_last_poll_timestamp_seconds`, `mixxx_ci_poll_duration_seconds`, `mixxx_ci_poll_errors_total` | |

To change the dashboard, edit `deploy/grafana/build_dashboard.py` and run it; Grafana picks up the new JSON
within a minute.

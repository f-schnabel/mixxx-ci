#!/usr/bin/env python3
"""Live monitor for the mixxxdj GitHub Actions runner queue.

    python monitor.py            # serve the dashboard on http://127.0.0.1:8765
    python monitor.py --once     # print one snapshot and exit

Endpoints: / (dashboard), /api/state (JSON), /metrics (Prometheus).
Uses $GITHUB_TOKEN, or the token of the logged-in GitHub CLI (`gh auth token`).
"""

import argparse
import datetime as dt
import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

ORG = 'mixxxdj'
API = 'https://api.github.com'
HERE = Path(__file__).resolve().parent
ALWAYS_WATCHED = ['mixxx', 'vcpkg']
ACTIVE_REPO_DAYS = 7
HISTORY_LEN = 14 * 24 * 60  # two weeks at one poll per minute
RUN_STATUSES = ['queued', 'in_progress', 'pending', 'waiting', 'requested']
FAMILIES = ['macOS', 'Linux', 'Windows', 'Other']
UTC = dt.timezone.utc


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')) if value else None


def runner_family(labels):
    text = ' '.join(labels).lower()
    for key, name in (('macos', 'macOS'), ('windows', 'Windows'), ('ubuntu', 'Linux')):
        if key in text:
            return name
    return 'Other'


def github_token():
    token = os.environ.get('GITHUB_TOKEN', '').strip()
    if token:
        return token
    try:
        result = subprocess.run(['gh', 'auth', 'token'], capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        raise SystemExit('Set GITHUB_TOKEN or log in with the GitHub CLI (gh auth login).') from None
    return result.stdout.strip()


class GitHub:
    def __init__(self):
        self.token = github_token()
        self.rate_remaining = None

    def get(self, path):
        request = Request(
            API + path,
            headers={
                'Authorization': f'Bearer {self.token}',
                'Accept': 'application/vnd.github+json',
                'X-GitHub-Api-Version': '2022-11-28',
                'User-Agent': 'mixxx-ci-queue-monitor',
            },
        )
        with urlopen(request, timeout=30) as response:
            self.rate_remaining = response.headers.get('X-RateLimit-Remaining')
            return json.load(response)


class Monitor:
    def __init__(self, args):
        self.args = args
        self.gh = GitHub()
        self.me = self.gh.get('/user')['login']
        self.repos = []
        self.repos_checked = 0.0
        self.snapshot = None
        self.error = None
        self.last_success = None
        self.poll_duration = None
        self.poll_errors = 0
        self.history_file = Path(args.history_file)
        self.history = self.load_history()
        self.lock = threading.Lock()

    def load_history(self):
        if not self.history_file.exists():
            return []
        lines = self.history_file.read_text(encoding='utf-8').splitlines()[-HISTORY_LEN:]
        history = [json.loads(line) for line in lines if line.strip()]
        self.history_file.write_text(''.join(json.dumps(p) + '\n' for p in history), encoding='utf-8')
        return history

    def watched_repos(self):
        if time.time() - self.repos_checked > 600:
            cutoff = dt.datetime.now(UTC) - dt.timedelta(days=ACTIVE_REPO_DAYS)
            repos = self.gh.get(f'/orgs/{ORG}/repos?sort=pushed&per_page=100')
            active = [r['name'] for r in repos if not r['archived'] and parse_time(r['pushed_at']) > cutoff]
            self.repos = sorted(set(active) | set(ALWAYS_WATCHED))
            self.repos_checked = time.time()
        return self.repos

    def take_snapshot(self):
        repos = self.watched_repos()
        with ThreadPoolExecutor(8) as pool:
            listings = pool.map(
                lambda rs: (rs[0], self.gh.get(f'/repos/{ORG}/{rs[0]}/actions/runs?status={rs[1]}&per_page=100')),
                [(repo, status) for repo in repos for status in RUN_STATUSES],
            )
            runs = {}
            for repo, listing in listings:
                for run in listing['workflow_runs']:
                    runs[run['id']] = (repo, run)
            # The newest run per pull request, including finished ones, so an old
            # run still waiting in the queue is marked even when its successor
            # already failed or was cancelled.
            recent_pr_runs = pool.map(
                lambda repo: (repo, self.gh.get(f'/repos/{ORG}/{repo}/actions/runs?event=pull_request&per_page=100')),
                repos,
            )
            recent_pr_runs = [(repo, run) for repo, listing in recent_pr_runs for run in listing['workflow_runs']]
            job_lists = pool.map(
                lambda item: (item, self.gh.get(f'/repos/{ORG}/{item[0]}/actions/runs/{item[1]["id"]}/jobs?per_page=100')),
                list(runs.values()),
            )
            job_lists = list(job_lists)

        # A pull request run is superseded when a newer run of the same workflow
        # exists for the same head branch.
        newest = {}
        for repo, run in [*runs.values(), *recent_pr_runs]:
            key = self.run_key(repo, run)
            if key and (key not in newest or run['created_at'] > newest[key]):
                newest[key] = run['created_at']

        jobs = []
        for (repo, run), listing in job_lists:
            key = self.run_key(repo, run)
            superseded = bool(key) and run['created_at'] < newest[key]
            for job in listing['jobs']:
                if job['status'] == 'completed':
                    continue
                jobs.append({
                    'repo': repo,
                    'run_id': run['id'],
                    'run_url': run['html_url'],
                    'run_title': run['display_title'],
                    'workflow': run['name'],
                    'branch': run['head_branch'],
                    'event': run['event'],
                    'actor': (run.get('triggering_actor') or run.get('actor') or {}).get('login'),
                    'name': job['name'],
                    'url': job['html_url'],
                    'status': job['status'],
                    'labels': job['labels'],
                    'family': runner_family(job['labels']),
                    'created_at': job['created_at'],
                    'started_at': job['started_at'] if job['status'] == 'in_progress' else None,
                    'superseded': superseded,
                })
        return {
            'taken_at': dt.datetime.now(UTC).isoformat(),
            'repos': repos,
            'me': self.me,
            'rate_remaining': self.gh.rate_remaining,
            'limits': {'total': self.args.total_slots, 'macOS': self.args.macos_slots},
            'macos_minutes': self.args.macos_minutes,
            'jobs': jobs,
        }

    @staticmethod
    def run_key(repo, run):
        if run['event'] != 'pull_request':
            return None
        head = (run.get('head_repository') or {}).get('full_name')
        return (repo, run['workflow_id'], head, run['head_branch'])

    def refresh(self):
        started = time.monotonic()
        snapshot = self.take_snapshot()
        jobs = snapshot['jobs']
        point = {
            't': snapshot['taken_at'],
            'running': sum(j['status'] == 'in_progress' for j in jobs),
            'queued': sum(j['status'] == 'queued' for j in jobs),
            'mac_running': sum(j['status'] == 'in_progress' and j['family'] == 'macOS' for j in jobs),
            'mac_queued': sum(j['status'] == 'queued' and j['family'] == 'macOS' for j in jobs),
        }
        with self.lock:
            self.snapshot = snapshot
            self.error = None
            self.last_success = time.time()
            self.poll_duration = time.monotonic() - started
            self.history = [*self.history, point][-HISTORY_LEN:]
        with self.history_file.open('a', encoding='utf-8') as file:
            file.write(json.dumps(point) + '\n')

    def poll_forever(self):
        while True:
            try:
                self.refresh()
            except Exception as error:  # keep polling through network or API hiccups
                with self.lock:
                    self.error = f'{type(error).__name__}: {error}'
                    self.poll_errors += 1
            time.sleep(self.args.interval)

    def state(self):
        with self.lock:
            return {
                'snapshot': self.snapshot,
                'history': self.history,
                'error': self.error,
                'interval': self.args.interval,
            }

    def metrics(self):
        with self.lock:
            snapshot, last_success = self.snapshot, self.last_success
            poll_duration, poll_errors = self.poll_duration, self.poll_errors
        lines = []

        def metric(name, kind, help_text, samples):
            lines.append(f'# HELP {name} {help_text}')
            lines.append(f'# TYPE {name} {kind}')
            for labels, value in samples:
                label_text = ','.join(f'{k}="{escape_label(v)}"' for k, v in labels.items())
                lines.append(f'{name}{{{label_text}}} {value}' if label_text else f'{name} {value}')

        metric('mixxx_ci_poll_errors_total', 'counter', 'Failed GitHub polls since start.', [({}, poll_errors)])
        metric('mixxx_ci_runner_limit', 'gauge', 'Concurrent job limit of the org.', [
            ({'runner': 'all'}, self.args.total_slots),
            ({'runner': 'macOS'}, self.args.macos_slots),
        ])
        if snapshot is None:
            return '\n'.join(lines) + '\n'

        jobs = snapshot['jobs']
        now = dt.datetime.now(UTC)
        status_of = {'in_progress': 'running', 'queued': 'queued'}

        def count(pred):
            return sum(1 for j in jobs if pred(j))

        per_family = []
        superseded = []
        oldest = []
        for family in FAMILIES:
            for status, gh_status in (('running', 'in_progress'), ('queued', 'queued')):
                per_family.append(({'runner': family, 'status': status},
                                   count(lambda j: j['family'] == family and j['status'] == gh_status)))
                superseded.append(({'runner': family, 'status': status},
                                   count(lambda j: j['family'] == family and j['status'] == gh_status
                                         and j['superseded'])))
            waits = [(now - parse_time(j['created_at'])).total_seconds()
                     for j in jobs if j['family'] == family and j['status'] == 'queued']
            oldest.append(({'runner': family}, round(max(waits, default=0))))

        by_label = {}
        for job in jobs:
            status = status_of.get(job['status'])
            if status is None:
                continue
            key = (job['repo'], ','.join(job['labels']), status)
            by_label[key] = by_label.get(key, 0) + 1

        mine = [({'status': s}, count(lambda j: j['actor'] == self.me and j['status'] == g))
                for s, g in (('running', 'in_progress'), ('queued', 'queued'))]

        metric('mixxx_ci_jobs', 'gauge', 'Unfinished jobs by runner type and status.', per_family)
        metric('mixxx_ci_jobs_by_label', 'gauge', 'Unfinished jobs by repository and runner label.',
               [({'repo': r, 'label': lbl, 'status': s}, n) for (r, lbl, s), n in sorted(by_label.items())])
        metric('mixxx_ci_superseded_jobs', 'gauge',
               'Unfinished jobs of pull request runs that already have a newer run.', superseded)
        metric('mixxx_ci_oldest_queued_seconds', 'gauge', 'Wait time of the oldest queued job.', oldest)
        metric('mixxx_ci_my_jobs', 'gauge', 'Unfinished jobs triggered by the token owner.', mine)
        if snapshot['rate_remaining'] is not None:
            metric('mixxx_ci_github_rate_remaining', 'gauge', 'GitHub API requests left this hour.',
                   [({}, snapshot['rate_remaining'])])
        metric('mixxx_ci_last_poll_timestamp_seconds', 'gauge', 'Time of the last successful poll.',
               [({}, round(last_success, 3))])
        metric('mixxx_ci_poll_duration_seconds', 'gauge', 'Duration of the last successful poll.',
               [({}, round(poll_duration, 3))])
        return '\n'.join(lines) + '\n'


def escape_label(value):
    return str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')


def print_summary(snapshot):
    jobs = snapshot['jobs']
    now = dt.datetime.now(UTC)
    running = [j for j in jobs if j['status'] == 'in_progress']
    queued = [j for j in jobs if j['status'] == 'queued']
    print(f'{now:%H:%M} UTC  running {len(running)}/{snapshot["limits"]["total"]}  queued {len(queued)}')
    for family in FAMILIES:
        r = [j for j in running if j['family'] == family]
        q = sorted((j for j in queued if j['family'] == family), key=lambda j: j['created_at'])
        if not r and not q:
            continue
        wait = int((now - parse_time(q[0]['created_at'])).total_seconds() // 60) if q else 0
        oldest = f'  oldest wait {wait} min' if q else ''
        print(f'  {family:8} running {len(r):3}  queued {len(q):3}{oldest}')
    superseded = [j for j in jobs if j['superseded']]
    print(f'  jobs of superseded PR runs: {len(superseded)}')
    print(f'  API requests left this hour: {snapshot["rate_remaining"]}')


def make_handler(monitor):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            if path == '/':
                self.send(200, 'text/html; charset=utf-8', (HERE / 'index.html').read_bytes())
            elif path == '/api/state':
                self.send(200, 'application/json', json.dumps(monitor.state()).encode())
            elif path == '/metrics':
                self.send(200, 'text/plain; version=0.0.4; charset=utf-8', monitor.metrics().encode())
            else:
                self.send(404, 'text/plain', b'not found')

        def send(self, code, content_type, body):
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--interval', type=int, default=60, help='seconds between GitHub polls')
    parser.add_argument('--total-slots', type=int, default=60, help='concurrent jobs for the org')
    parser.add_argument('--macos-slots', type=int, default=5, help='concurrent macOS jobs for the org')
    parser.add_argument('--macos-minutes', type=int, default=35, help='average macOS job duration for estimates')
    parser.add_argument('--history-file', default=str(HERE / 'history.jsonl'), help='where the chart history is kept')
    parser.add_argument('--once', action='store_true', help='print one snapshot and exit')
    args = parser.parse_args()

    monitor = Monitor(args)
    if args.once:
        print_summary(monitor.take_snapshot())
        return
    threading.Thread(target=monitor.poll_forever, daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(monitor))
    print(f'CI queue monitor on http://{args.host}:{args.port} (polling every {args.interval}s)', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Live monitor for the mixxxdj GitHub Actions runner queue.

    python monitor.py            # serve the dashboard on http://127.0.0.1:8765
    python monitor.py --once     # poll once, print a summary and exit

Endpoints: / (dashboard), /api/state (current jobs), /api/history?hours=24 (chart data).

Runs and jobs are cached in SQLite. A run's jobs are only fetched again when the
run's updated_at changes, and the chart is computed from the jobs' created,
started and completed times, so it also covers the days before the monitor ran.
Uses $GITHUB_TOKEN, or the token of the logged-in GitHub CLI (`gh auth token`).
"""

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

ORG = 'mixxxdj'
API = 'https://api.github.com'
HERE = Path(__file__).resolve().parent
ALWAYS_WATCHED = ['mixxx', 'vcpkg']
ACTIVE_REPO_DAYS = 7
FAMILIES = ['macOS', 'Linux', 'Windows', 'Other']
HISTORY_POINTS = 720
BACKFILL_RESERVE = 1000  # API calls left for live polling while backfilling
UTC = dt.timezone.utc


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')) if value else None


def timestamp(value):
    return parse_time(value).timestamp() if value else None


def iso_date(ts):
    return dt.datetime.fromtimestamp(ts, UTC).strftime('%Y-%m-%d')


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
        self.rate_reset = 0

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
            self.rate_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            self.rate_reset = int(response.headers.get('X-RateLimit-Reset', 0))
            return json.load(response)

    def wait_for_budget(self, reserve):
        """Sleeps until the rate limit resets when fewer than `reserve` calls are left."""
        if self.rate_remaining is not None and self.rate_remaining < reserve:
            time.sleep(max(0, self.rate_reset - time.time()) + 5)


class Store:
    RUN_FIELDS = ['id', 'repo', 'workflow_id', 'workflow', 'event', 'head_repo', 'head_branch', 'title', 'url',
                  'actor', 'created_at', 'updated_at', 'status']
    JOB_FIELDS = ['id', 'run_id', 'name', 'url', 'labels', 'runner_name', 'status', 'conclusion', 'created_at',
                  'started_at', 'completed_at']

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock, self.db:
            self.db.executescript('''
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY, repo TEXT, workflow_id INTEGER, workflow TEXT, event TEXT,
                    head_repo TEXT, head_branch TEXT, title TEXT, url TEXT, actor TEXT,
                    created_at TEXT, updated_at TEXT, status TEXT,
                    jobs_synced TEXT  -- the run's updated_at when its jobs were last fetched
                );
                CREATE INDEX IF NOT EXISTS runs_created ON runs (created_at);
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY, run_id INTEGER, name TEXT, url TEXT, labels TEXT, runner_name TEXT,
                    status TEXT, conclusion TEXT, created_at TEXT, started_at TEXT, completed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS jobs_run ON jobs (run_id);
                -- Past days whose runs were listed completely; no new runs appear there.
                CREATE TABLE IF NOT EXISTS listed_days (repo TEXT, day TEXT, PRIMARY KEY (repo, day));
            ''')

    def save_runs(self, repo, runs):
        rows = [(
            run['id'], repo, run['workflow_id'], run['name'], run['event'],
            (run.get('head_repository') or {}).get('full_name'), run['head_branch'], run['display_title'],
            run['html_url'], (run.get('triggering_actor') or run.get('actor') or {}).get('login'),
            run['created_at'], run['updated_at'], run['status'],
        ) for run in runs]
        updates = ', '.join(f'{f} = excluded.{f}' for f in self.RUN_FIELDS[1:])
        with self.lock, self.db:
            self.db.executemany(
                f'INSERT INTO runs ({", ".join(self.RUN_FIELDS)}) VALUES ({", ".join("?" * len(self.RUN_FIELDS))}) '
                f'ON CONFLICT (id) DO UPDATE SET {updates}', rows)

    def save_jobs(self, run_id, synced_at, jobs):
        rows = [(
            job['id'], run_id, job['name'], job['html_url'], json.dumps(job['labels']), job['runner_name'],
            job['status'],
            job['conclusion'], job['created_at'], job['started_at'], job['completed_at'],
        ) for job in jobs]
        with self.lock, self.db:
            self.db.executemany(f'INSERT OR REPLACE INTO jobs VALUES ({", ".join("?" * len(self.JOB_FIELDS))})', rows)
            self.db.execute('UPDATE runs SET jobs_synced = ? WHERE id = ?', (synced_at, run_id))

    def listed_days(self):
        with self.lock:
            return {(r['repo'], r['day']) for r in self.db.execute('SELECT repo, day FROM listed_days')}

    def mark_listed(self, repo, day):
        with self.lock, self.db:
            self.db.execute('INSERT OR IGNORE INTO listed_days VALUES (?, ?)', (repo, day))

    def active_runs(self):
        with self.lock:
            return [(r['id'], r['repo']) for r in self.db.execute("SELECT id, repo FROM runs WHERE status != 'completed'")]

    def runs_needing_jobs(self, since, only_active=False):
        """Runs whose jobs changed since they were last fetched, newest first."""
        query = ('SELECT id, repo, updated_at FROM runs WHERE created_at >= ? '
                 'AND (jobs_synced IS NULL OR jobs_synced != updated_at)')
        if only_active:
            # Finished runs seen for the first time are left to the backfill thread.
            query += " AND (status != 'completed' OR jobs_synced IS NOT NULL)"
        with self.lock:
            return [tuple(r) for r in self.db.execute(query + ' ORDER BY created_at DESC', (since,))]

    def delete_run(self, run_id):
        with self.lock, self.db:
            self.db.execute('DELETE FROM jobs WHERE run_id = ?', (run_id,))
            self.db.execute('DELETE FROM runs WHERE id = ?', (run_id,))

    def prune(self, before):
        with self.lock, self.db:
            self.db.execute("DELETE FROM jobs WHERE run_id IN "
                            "(SELECT id FROM runs WHERE created_at < ? AND status = 'completed')", (before,))
            self.db.execute("DELETE FROM runs WHERE created_at < ? AND status = 'completed'", (before,))

    def load(self, since):
        with self.lock:
            runs = {r['id']: dict(r) for r in self.db.execute(
                "SELECT * FROM runs WHERE created_at >= ? OR status != 'completed'", (since,))}
            jobs = [dict(j) for j in self.db.execute(
                'SELECT jobs.* FROM jobs JOIN runs ON runs.id = jobs.run_id '
                "WHERE runs.created_at >= ? OR runs.status != 'completed'", (since,))]
            # The history is complete from the newest run whose jobs are still
            # missing; runs of the last hour are fetched on the next round.
            recent = dt.datetime.fromtimestamp(time.time() - 3600, UTC).isoformat()
            gap = self.db.execute(
                'SELECT MAX(created_at) FROM runs WHERE jobs_synced IS NULL AND created_at >= ? AND created_at < ?',
                (since, recent)).fetchone()[0]
            covered = gap or self.db.execute(
                'SELECT MIN(created_at) FROM runs WHERE jobs_synced IS NOT NULL AND created_at >= ?',
                (since,)).fetchone()[0]
        return runs, jobs, covered


class Monitor:
    def __init__(self, args):
        self.args = args
        self.gh = GitHub()
        self.me = self.gh.get('/user')['login']
        self.store = Store(Path(args.data_dir) / 'cache.sqlite')
        self.org_repos = []
        self.org_repos_checked = 0.0
        self.snapshot = None
        self.model = None
        self.history_cache = {}
        self.error = None
        self.backfill_status = 'waiting'
        self.lock = threading.Lock()

    def window_start(self):
        return time.time() - self.args.days * 86400

    def watched_repos(self, days):
        if time.time() - self.org_repos_checked > 600:
            self.org_repos = [r for r in self.gh.get(f'/orgs/{ORG}/repos?sort=pushed&per_page=100')
                              if not r['archived']]
            self.org_repos_checked = time.time()
        cutoff = time.time() - days * 86400
        active = [r['name'] for r in self.org_repos if timestamp(r['pushed_at']) > cutoff]
        return sorted(set(active) | set(ALWAYS_WATCHED))

    def sync_jobs(self, run):
        run_id, repo, updated_at = run
        jobs, page = [], 1
        try:
            while True:
                listing = self.gh.get(
                    f'/repos/{ORG}/{repo}/actions/runs/{run_id}/jobs?filter=all&per_page=100&page={page}')
                jobs += listing['jobs']
                if len(jobs) >= listing['total_count'] or not listing['jobs']:
                    break
                page += 1
        except HTTPError as error:
            if error.code == 404:
                self.store.delete_run(run_id)
                return
            raise
        self.store.save_jobs(run_id, updated_at, jobs)

    def poll(self):
        repos = self.watched_repos(ACTIVE_REPO_DAYS)
        since = dt.datetime.fromtimestamp(self.window_start(), UTC).isoformat()
        with ThreadPoolExecutor(8) as pool:
            seen = set()
            for repo, runs in pool.map(
                    lambda r: (r, self.gh.get(f'/repos/{ORG}/{r}/actions/runs?per_page=100')['workflow_runs']), repos):
                self.store.save_runs(repo, runs)
                seen |= {run['id'] for run in runs}
            # Active runs that dropped off the first page, e.g. long vcpkg builds
            stale = [(run_id, repo) for run_id, repo in self.store.active_runs() if run_id not in seen]
            for repo, run in pool.map(self.fetch_run, stale):
                if run is not None:
                    self.store.save_runs(repo, [run])
            list(pool.map(self.sync_jobs, self.store.runs_needing_jobs(since, only_active=True)))
        self.rebuild()

    def fetch_run(self, item):
        run_id, repo = item
        try:
            return repo, self.gh.get(f'/repos/{ORG}/{repo}/actions/runs/{run_id}')
        except HTTPError as error:
            if error.code == 404:
                self.store.delete_run(run_id)
                return repo, None
            raise

    def backfill_forever(self):
        """Fills the cache with the runs of the last days, then keeps catching up
        on runs that finished between two polls."""
        while True:
            try:
                start = self.window_start()
                today = iso_date(time.time())
                done = self.store.listed_days()
                days = [iso_date(start + i * 86400) for i in range(self.args.days + 1)]
                todo = [(repo, day) for repo in self.watched_repos(self.args.days) for day in days
                        if day <= today and (repo, day) not in done]
                self.backfill_status = f'listing runs of {len(todo)} repository days'
                self.in_batches(self.list_runs_of_day, todo)
                since = dt.datetime.fromtimestamp(start, UTC).isoformat()
                runs = self.store.runs_needing_jobs(since)
                self.backfill_status = f'fetching jobs of {len(runs)} runs'
                self.in_batches(self.sync_jobs, runs, rebuild=True)
                self.store.prune(since)
                self.backfill_status = 'complete'
                self.rebuild()
            except Exception as error:  # retry later
                self.backfill_status = f'retrying after {type(error).__name__}: {error}'
            time.sleep(60)

    def in_batches(self, work, items, rebuild=False, size=40):
        with ThreadPoolExecutor(8) as pool:
            for i in range(0, len(items), size):
                self.gh.wait_for_budget(BACKFILL_RESERVE)
                list(pool.map(work, items[i:i + size]))
                if rebuild:
                    self.backfill_status = f'fetching jobs of {max(0, len(items) - i - size)} runs'
                    if i % (5 * size) == 0:
                        self.rebuild()

    def list_runs_of_day(self, item):
        # The runs API returns at most 1000 results per query, so list one day at a time.
        repo, day = item
        page = 1
        while True:
            runs = self.gh.get(
                f'/repos/{ORG}/{repo}/actions/runs?created={day}&per_page=100&page={page}')['workflow_runs']
            self.store.save_runs(repo, runs)
            if len(runs) < 100:
                break
            page += 1
        if day < iso_date(time.time()):
            self.store.mark_listed(repo, day)

    def rebuild(self):
        """Recomputes the current state from the cache."""
        since = dt.datetime.fromtimestamp(self.window_start(), UTC).isoformat()
        runs, jobs, covered = self.store.load(since)
        now = time.time()

        # A pull request run is superseded from the moment a newer run of the same
        # workflow for the same head branch was created.
        by_key = {}
        for run in runs.values():
            if run['event'] == 'pull_request':
                key = (run['repo'], run['workflow_id'], run['head_repo'], run['head_branch'])
                by_key.setdefault(key, []).append(run)
        superseded_since = {}
        for group in by_key.values():
            group.sort(key=lambda r: r['created_at'])
            for older, newer in zip(group, group[1:]):
                superseded_since[older['id']] = timestamp(newer['created_at'])

        model = []
        seen = set()
        for job in jobs:
            run = runs.get(job['run_id'])
            labels = json.loads(job['labels'])
            if run is None or not labels or job['conclusion'] == 'skipped':
                continue
            # A re-run copies the jobs it does not repeat into the new attempt,
            # with the same runner and times.
            if job['runner_name']:
                key = (job['run_id'], job['name'], job['runner_name'], job['started_at'])
                if key in seen:
                    continue
                seen.add(key)
            # GitHub fills started_at already while a job is queued; a job only
            # ran when a runner picked it up.
            ran = job['status'] == 'in_progress' or (job['status'] == 'completed' and bool(job['runner_name']))
            model.append({
                'job': job, 'run': run, 'labels': labels, 'family': runner_family(labels),
                'created': timestamp(job['created_at']), 'started': timestamp(job['started_at']) if ran else None,
                'completed': timestamp(job['completed_at']) if job['status'] == 'completed' else None,
                'superseded_since': superseded_since.get(run['id']),
            })

        current = []
        for m in model:
            job, run = m['job'], m['run']
            if job['status'] == 'completed':
                continue
            current.append({
                'repo': run['repo'], 'run_id': run['id'], 'run_url': run['url'], 'run_title': run['title'],
                'workflow': run['workflow'], 'branch': run['head_branch'], 'event': run['event'],
                'actor': run['actor'], 'name': job['name'], 'url': job['url'], 'status': job['status'],
                'labels': m['labels'], 'family': m['family'], 'created_at': job['created_at'],
                'started_at': job['started_at'] if job['status'] == 'in_progress' else None,
                'superseded': m['superseded_since'] is not None,
            })

        snapshot = {
            'taken_at': dt.datetime.now(UTC).isoformat(),
            'repos': self.watched_repos(ACTIVE_REPO_DAYS),
            'me': self.me,
            'rate_remaining': self.gh.rate_remaining,
            'limits': {'total': self.args.total_slots, 'macOS': self.args.macos_slots},
            'history_from': covered,
            'backfill': self.backfill_status,
            'macos': self.macos_stats(model, now),
            'jobs': current,
        }
        with self.lock:
            self.model = (model, now, timestamp(covered) or now)
            self.snapshot = snapshot
            self.history_cache = {}
            self.error = None

    @staticmethod
    def macos_stats(model, now):
        mac = [m for m in model if m['family'] == 'macOS']
        ran = [m for m in mac if m['started'] and m['completed']]
        durations = [(m['completed'] - m['started']) / 60 for m in ran
                     if m['completed'] > now - 86400 and m['completed'] - m['started'] > 120]
        hours = 6
        return {
            'median_minutes': round(statistics.median(durations)) if durations else None,
            'added_per_hour': round(sum(1 for m in mac if m['created'] > now - hours * 3600) / hours, 1),
            'finished_per_hour': round(sum(1 for m in ran if m['completed'] > now - hours * 3600) / hours, 1),
        }

    def history(self, hours):
        with self.lock:
            if hours in self.history_cache:
                return self.history_cache[hours]
            model, now, covered = self.model or ([], time.time(), time.time())
        start = max(now - hours * 3600, covered)
        step = max(60, math.ceil((now - start) / HISTORY_POINTS / 60) * 60)
        count = int((now - start) // step) + 1
        open_end = now + step  # jobs that are still queued or running count at the last sample
        keys = ['running', 'queued', 'mac_running', 'mac_queued', 'superseded_queued']
        series = {k: [0] * count for k in keys}
        oldest_mac = [None] * count

        def span(a, b):
            """Indices of the sample times within [a, b)."""
            return range(max(0, math.ceil((a - start) / step)), min(count, math.ceil((b - start) / step)))

        for m in model:
            queued_until = m['started'] or m['completed'] or open_end
            mac = m['family'] == 'macOS'
            for i in span(m['created'], queued_until):
                series['queued'][i] += 1
                if mac:
                    series['mac_queued'][i] += 1
                    if oldest_mac[i] is None or m['created'] < oldest_mac[i]:
                        oldest_mac[i] = m['created']
            if m['superseded_since'] is not None:
                for i in span(max(m['created'], m['superseded_since']), queued_until):
                    series['superseded_queued'][i] += 1
            if m['started']:
                for i in span(m['started'], m['completed'] or open_end):
                    series['running'][i] += 1
                    if mac:
                        series['mac_running'][i] += 1

        points = []
        for i in range(count):
            t = start + i * step
            point = {'t': round(t * 1000), **{k: series[k][i] for k in keys}}
            point['mac_wait_minutes'] = round((t - oldest_mac[i]) / 60) if oldest_mac[i] else 0
            points.append(point)
        result = {'step': step, 'from': round(start * 1000), 'points': points}
        with self.lock:
            self.history_cache[hours] = result
        return result

    def poll_forever(self):
        while True:
            try:
                self.poll()
            except Exception as error:  # keep polling through network or API hiccups
                with self.lock:
                    self.error = f'{type(error).__name__}: {error}'
            time.sleep(self.args.interval)

    def state(self):
        with self.lock:
            return {'snapshot': self.snapshot, 'error': self.error, 'interval': self.args.interval,
                    'days': self.args.days}


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
    print(f'  jobs of superseded PR runs: {sum(j["superseded"] for j in jobs)}')
    print(f'  macOS: {snapshot["macos"]}')
    print(f'  API requests left this hour: {snapshot["rate_remaining"]}')


def make_handler(monitor):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path == '/':
                self.send(200, 'text/html; charset=utf-8', (HERE / 'index.html').read_bytes())
            elif url.path == '/api/state':
                self.send_json(monitor.state())
            elif url.path == '/api/history':
                try:
                    hours = float(parse_qs(url.query).get('hours', ['24'])[0])
                except ValueError:
                    hours = 24
                hours = min(max(hours, 1), monitor.args.days * 24)
                self.send_json(monitor.history(hours))
            else:
                self.send(404, 'text/plain', b'not found')

        def send_json(self, data):
            self.send(200, 'application/json', json.dumps(data).encode())

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
    parser.add_argument('--days', type=int, default=14, help='how many days of history to keep')
    parser.add_argument('--total-slots', type=int, default=60, help='concurrent jobs for the org')
    parser.add_argument('--macos-slots', type=int, default=5, help='concurrent macOS jobs for the org')
    parser.add_argument('--data-dir', default=str(HERE / 'data'), help='where the cache is kept')
    parser.add_argument('--once', action='store_true', help='poll once, print a summary and exit')
    args = parser.parse_args()

    monitor = Monitor(args)
    if args.once:
        monitor.poll()
        print_summary(monitor.snapshot)
        return
    threading.Thread(target=monitor.poll_forever, daemon=True).start()
    threading.Thread(target=monitor.backfill_forever, daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(monitor))
    print(f'CI queue monitor on http://{args.host}:{args.port} (polling every {args.interval}s)', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()

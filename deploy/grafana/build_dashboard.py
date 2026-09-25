"""Writes dashboards/mixxx-ci-queue.json. Run after changing the panels here."""

import json
from pathlib import Path

DS = {'type': 'prometheus', 'uid': '${datasource}'}
MACOS_JOB_MINUTES = 35
_ids = iter(range(1, 100))


def thresholds(*steps):
    return {'mode': 'absolute', 'steps': [{'color': color, 'value': value} for value, color in steps]}


def panel(kind, title, targets, grid, instant=False, **extra):
    x, y, w, h = grid
    return {
        'id': next(_ids),
        'type': kind,
        'title': title,
        'datasource': DS,
        'gridPos': {'x': x, 'y': y, 'w': w, 'h': h},
        'targets': [
            {'datasource': DS, 'refId': chr(65 + i), 'expr': expr, 'legendFormat': legend,
             'instant': instant, 'range': not instant}
            for i, (expr, legend) in enumerate(targets)
        ],
        **extra,
    }


def single(kind, title, expr, grid, unit='none', steps=((None, 'green'),), description='', **field):
    options = {'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}
    if kind == 'stat':
        options.update(colorMode='value', graphMode='area', textMode='value', justifyMode='auto', orientation='auto')
    else:
        options.update(showThresholdMarkers=True, showThresholdLabels=False)
    return panel(kind, title, [(expr, '')], grid, description=description, options=options, fieldConfig={
        'defaults': {'unit': unit, 'thresholds': thresholds(*steps), 'color': {'mode': 'thresholds'}, **field},
        'overrides': [],
    })


def series(title, targets, grid, unit='none', stack=False, description=''):
    return panel('timeseries', title, targets, grid, description=description, fieldConfig={
        'defaults': {
            'unit': unit,
            'min': 0,
            'color': {'mode': 'palette-classic'},
            'custom': {
                'drawStyle': 'line', 'lineInterpolation': 'stepAfter', 'lineWidth': 2,
                'fillOpacity': 20 if stack else 0, 'showPoints': 'never', 'spanNulls': False,
                'stacking': {'mode': 'normal' if stack else 'none', 'group': 'A'},
            },
        },
        'overrides': [],
    }, options={
        'legend': {'displayMode': 'list', 'placement': 'bottom', 'showLegend': True},
        'tooltip': {'mode': 'multi', 'sort': 'desc'},
    })


RED_AFTER_HOURS = ((None, 'green'), (3600, 'yellow'), (7200, 'red'))

panels = [
    single('gauge', 'Org runners in use', 'sum(mixxx_ci_jobs{status="running"})', (0, 0, 4, 5),
           steps=((None, 'green'), (48, 'orange'), (60, 'red')), min=0, max=60),
    single('gauge', 'macOS runners in use', 'sum(mixxx_ci_jobs{runner="macOS",status="running"})', (4, 0, 4, 5),
           steps=((None, 'green'), (5, 'orange')), min=0, max=5),
    single('stat', 'macOS jobs queued', 'sum(mixxx_ci_jobs{runner="macOS",status="queued"})', (8, 0, 4, 5),
           steps=((None, 'green'), (1, 'yellow'), (10, 'red'))),
    single('stat', 'Oldest macOS wait', 'max(mixxx_ci_oldest_queued_seconds{runner="macOS"})', (12, 0, 4, 5),
           unit='s', steps=RED_AFTER_HOURS),
    single('stat', 'macOS backlog',
           f'sum(mixxx_ci_jobs{{runner="macOS",status="queued"}}) * {MACOS_JOB_MINUTES * 60}'
           ' / scalar(mixxx_ci_runner_limit{runner="macOS"})',
           (16, 0, 4, 5), unit='s', steps=RED_AFTER_HOURS,
           description=f'Time to clear the queued macOS jobs at ~{MACOS_JOB_MINUTES} min per job, without new pushes.'),
    single('stat', 'Superseded jobs', 'sum(mixxx_ci_superseded_jobs)', (20, 0, 4, 5),
           steps=((None, 'green'), (1, 'yellow'), (10, 'red')),
           description='Unfinished jobs of pull request runs that already have a newer run of the same PR.'),
    series('Queued jobs by runner', [('sum by (runner) (mixxx_ci_jobs{status="queued"})', '{{runner}}')],
           (0, 5, 12, 8), stack=True),
    series('Running jobs by runner', [
        ('sum by (runner) (mixxx_ci_jobs{status="running"})', '{{runner}}'),
        ('mixxx_ci_runner_limit{runner="macOS"}', 'macOS limit'),
    ], (12, 5, 12, 8), description='The org shares 60 concurrent jobs, at most 5 of them on macOS.'),
    series('Oldest queued job by runner', [('max by (runner) (mixxx_ci_oldest_queued_seconds)', '{{runner}}')],
           (0, 13, 12, 8), unit='s'),
    series('Superseded jobs', [
        ('sum by (status) (mixxx_ci_superseded_jobs)', 'superseded {{status}}'),
        ('sum by (status) (mixxx_ci_my_jobs)', 'mine {{status}}'),
    ], (12, 13, 12, 8), description='Jobs of outdated pull request runs, next to all jobs of the token owner.'),
    panel('bargauge', 'Jobs by runner label (now)',
          [('sum by (label, status) (mixxx_ci_jobs_by_label)', '{{label}} · {{status}}')], (0, 21, 12, 9),
          instant=True,
          fieldConfig={'defaults': {'min': 0, 'thresholds': thresholds((None, 'blue')),
                                    'color': {'mode': 'thresholds'}}, 'overrides': []},
          options={'orientation': 'horizontal', 'displayMode': 'basic', 'showUnfilled': True,
                   'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}),
    series('GitHub API calls left', [('mixxx_ci_github_rate_remaining', 'remaining')], (12, 21, 6, 9)),
    single('stat', 'Last poll', 'time() - mixxx_ci_last_poll_timestamp_seconds', (18, 21, 3, 9), unit='s',
           steps=((None, 'green'), (180, 'yellow'), (600, 'red'))),
    single('stat', 'Poll errors (1h)', 'increase(mixxx_ci_poll_errors_total[1h])', (21, 21, 3, 9),
           steps=((None, 'green'), (1, 'yellow'), (5, 'red'))),
]

dashboard = {
    'uid': 'mixxx-ci-queue',
    'title': 'mixxxdj CI queue',
    'tags': ['mixxx', 'github-actions'],
    'timezone': 'browser',
    'schemaVersion': 39,
    'version': 1,
    'editable': False,
    'graphTooltip': 1,
    'refresh': '1m',
    'time': {'from': 'now-24h', 'to': 'now'},
    'links': [{'title': 'Job list', 'type': 'link', 'url': 'https://mixxx-ci.schnabel.dev',
               'targetBlank': True, 'icon': 'external link'}],
    'templating': {'list': [{'name': 'datasource', 'label': 'Data source', 'type': 'datasource',
                             'query': 'prometheus', 'current': {}, 'hide': 0, 'refresh': 1}]},
    'annotations': {'list': []},
    'panels': panels,
}

out = Path(__file__).parent / 'dashboards' / 'mixxx-ci-queue.json'
out.write_text(json.dumps(dashboard, indent=2) + '\n', encoding='utf-8')
print(f'wrote {out} ({len(panels)} panels)')

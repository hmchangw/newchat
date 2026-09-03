# Dashboard lint

Static checks for the Grafana dashboard JSON under `deploy/grafana/dashboards`.

```sh
make lint-dashboards          # check every dashboard
make test-dashboards          # run the rules against their fixtures

python3 tools/dashboards/lint.py --columns deploy/grafana/dashboards/service-health.json
```

## Why it exists

A dashboard defect is not a crash. The JSON is valid, Grafana raises nothing,
and the board is simply wrong on screen: a column missing, a row collapsed into
another row's numbers, a filter that matches the whole cluster. Whoever edits
these files usually cannot see the rendered result, so the checks below are the
only feedback available before someone opens the board.

Every rule was written after the defect it catches shipped.

## Rules

| id | what it catches |
|---|---|
| `panel-missing-description` | a panel with nothing saying what an empty version of it means |
| `panel-missing-unit` | raw numbers with no unit |
| `panel-missing-novalue` | absent data rendering as blank, which reads as zero |
| `panel-datasource-not-variable` | a panel pinned to one datasource on a multi-site board |
| `target-datasource-not-variable` | the same, one level down |
| `target-unbalanced` | mismatched brackets |
| `target-hardcoded-range` | `[5m]` instead of `$__rate_interval`, so the panel ignores the zoom |
| `target-counter-not-aggregated` | a `_total` selector plotted as a level |
| `target-double-escaped` | a regex escaped twice through JSON: PromQL sees a literal backslash and the match never fires |
| `target-unwitnessed-zero` | a trailing `or vector(0)` turning "nothing scraped" into a confident zero (warning) |
| `variable-allvalue-wildcard` | `allValue: ".*"`, which makes All mean every value in the datastore rather than every option offered |
| `variable-undefined` | `$name` with no such variable |
| `layout-overlap` | panels on top of each other |
| `layout-incomplete-row` | a grid row narrower than 24, which reads as a missing panel |
| `table-join-key-missing` | joining on a field the first query does not produce |
| `table-join-key-not-unique` | rows identified by two labels joined on one of them, so rows sharing it collapse into one carrying another's numbers |
| `table-index-incomplete` | a partial `indexByName`: Grafana drops the fields it cannot order |
| `table-redundant-join` | joining a single frame, which only reorders its fields |
| `table-sortby-missing-column` | sorting on a column that is not rendered, which looks like no sorting at all |
| `table-override-missing-column` | a `byName` override aimed at a column that is not rendered |

## The column simulation

`simulate_columns` walks the Prometheus → `joinByField` → `organize` pipeline
and reports the columns a table actually renders, in order. That is what the
last five rules are built on, and `--columns` prints it:

```
HTTP routes   ['service', 'route', 'req/s', 'error %', 'p95']
```

It mirrors three behaviours that are easy to get wrong by reading the JSON: a
Prometheus table frame orders its label columns alphabetically, `joinByField`
suffixes a repeated field name rather than merging it, and `organize` orders on
the original field names and renames afterwards.

## Adding a rule

Add the check to `lint.py` and a fixture to `testdata/` declaring the rule ids
it must raise in a top-level `_expect` list. `lint_test.py` compares that set
exactly in both directions and fails on any rule with no fixture at all — an
unverified rule can be disabled by an edit with nothing turning red.

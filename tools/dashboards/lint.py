#!/usr/bin/env python3
"""Lint Grafana dashboard JSON.

Every rule here exists because the defect it catches shipped: the file was
valid JSON, Grafana raised nothing, and the board was wrong on screen. Static
checks are the only feedback available to whoever edits these files without a
cluster in front of them.

The interesting half is not the field checks but simulate_columns, which walks
the Prometheus -> joinByField -> organize pipeline and reports the columns a
table will actually render. Three separate defects in one board came from
guessing at that pipeline instead of computing it.

Usage:
    python3 tools/dashboards/lint.py [path ...]     # defaults to deploy/grafana/dashboards
    python3 tools/dashboards/lint.py --columns ...  # also print each table's columns
"""
import glob
import itertools
import json
import os
import re
import sys

DEFAULT_GLOB = "deploy/grafana/dashboards/*.json"
DATASOURCE_REF = "${datasource}"

# Grafana template variables and the Prometheus datasource's own macros. A name
# outside this set that appears as $name must be declared by the dashboard.
BUILTIN_VARS = {
    "__rate_interval", "__interval", "__interval_ms", "__range", "__range_s",
    "__range_ms", "__all", "__auto", "__from", "__to", "__dashboard",
    "__user", "__org", "__name", "__field", "__series", "__value", "__timeFilter",
}

AGG_BY = re.compile(r"(?:sum|min|max|avg|count|stddev|stdvar|topk|bottomk|quantile) by \(([^)]*)\)")
LABEL_JOIN = re.compile(r'label_join\(.*?,\s*"(\w+)"\s*,\s*"[^"]*"\s*((?:,\s*"\w+"\s*)+)\)')
HARDCODED_RANGE = re.compile(r"\[\d+[smhdwy]\]")
COUNTER_SELECTOR = re.compile(r"\b[a-z_][a-z0-9_]*_total\s*\{")
RATE_FN = re.compile(r"\b(?:rate|irate|increase|resets|changes)\s*\(")


class Finding:
    def __init__(self, rule, where, message, severity="error"):
        self.rule = rule
        self.where = where
        self.message = message
        self.severity = severity

    def __str__(self):
        return "%-7s %-32s %-28s %s" % (
            self.severity.upper(), self.rule, self.where[:28], self.message)


def walk_panels(dashboard):
    """Every non-row panel, including the ones nested inside a collapsed row."""
    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row":
            for nested in panel.get("panels", []):
                yield nested
        else:
            yield panel


def frame_labels(expr):
    """Labels a Prometheus table frame carries for this expression.

    The outermost aggregation decides them. `le` never survives into a table:
    histogram_quantile consumes it.
    """
    outer = AGG_BY.findall(expr)
    labels = set()
    if outer:
        # The leftmost by() in source order is the outermost aggregation:
        # sum by (x) ( ... sum by (y) (...) ... ). Reading the last one instead
        # reports an inner grouping the frame never sees.
        labels |= {part.strip() for part in outer[0].split(",") if part.strip()}
    for match in LABEL_JOIN.finditer(expr):
        labels.add(match.group(1))
    labels.discard("le")
    return labels


def joined_labels(expr):
    """The labels each label_join folds into its destination label."""
    folded = {}
    for match in LABEL_JOIN.finditer(expr):
        folded[match.group(1)] = set(re.findall(r'"(\w+)"', match.group(2)))
    return folded


def simulate_columns(panel):
    """The field list a table panel produces, before and after organize.

    Mirrors Grafana: a Prometheus table frame is Time, then its labels sorted
    by name, then Value; joinByField emits the join field, then each frame's
    remaining fields in order, suffixing a repeated name with a counter.
    """
    targets = panel.get("targets", [])
    multi = len(targets) > 1
    frames = []
    for target in targets:
        value = "Value #%s" % target.get("refId", "A") if multi else "Value"
        frames.append(["Time"] + sorted(frame_labels(target["expr"])) + [value])

    transforms = {t["id"]: t.get("options", {}) for t in panel.get("transformations", [])}
    join = transforms.get("joinByField", {}).get("byField")
    if join:
        fields, seen = [join], {join: 1}
        for frame in frames:
            for field in frame:
                if field == join:
                    continue
                if field in seen:
                    fields.append("%s %d" % (field, seen[field]))
                    seen[field] += 1
                else:
                    fields.append(field)
                    seen[field] = 1
    else:
        fields = list(frames[0]) if frames else []

    organize = transforms.get("organize", {})
    excluded = organize.get("excludeByName", {})
    index = organize.get("indexByName", {})
    renames = organize.get("renameByName", {})

    visible = [f for f in fields if not excluded.get(f)]
    if index:
        visible.sort(key=lambda f: index.get(f, len(fields) + 1))
    return fields, [renames.get(f, f) for f in visible]


def check_variables(dashboard, findings):
    variables = {v["name"]: v for v in dashboard.get("templating", {}).get("list", [])}
    for name, var in variables.items():
        if var.get("allValue") == ".*":
            findings.append(Finding(
                "variable-allvalue-wildcard", "$" + name,
                "All then means every value in the datastore, not every option "
                "in this list; drop allValue and Grafana builds the union"))
    referenced = set()
    for match in re.finditer(r"\$\{(\w+)[:}]|\$([A-Za-z_]\w*)", json.dumps(dashboard)):
        # The alternation deliberately requires a leading letter: $1 in a
        # label_replace replacement is a regex backreference, not a variable.
        referenced.add(match.group(1) or match.group(2))
    for name in sorted(referenced - set(variables) - BUILTIN_VARS):
        findings.append(Finding("variable-undefined", "$" + name,
                                "referenced but not declared by the dashboard"))


def check_layout(dashboard, findings):
    groups = [(dashboard.get("panels", []), "dashboard")]
    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row" and panel.get("panels"):
            groups.append((panel["panels"], panel.get("title", "row")))

    def overlaps(a, b):
        return not (a["x"] + a["w"] <= b["x"] or b["x"] + b["w"] <= a["x"]
                    or a["y"] + a["h"] <= b["y"] or b["y"] + b["h"] <= a["y"])

    for panels, label in groups:
        boxes = [(p["gridPos"], p.get("title", "?")) for p in panels if "gridPos" in p]
        for (ga, ta), (gb, tb) in itertools.combinations(boxes, 2):
            if overlaps(ga, gb):
                findings.append(Finding("layout-overlap", label,
                                        "%s overlaps %s" % (ta, tb)))
        widths = {}
        for grid, _ in boxes:
            widths[grid["y"]] = widths.get(grid["y"], 0) + grid["w"]
        for y, total in sorted(widths.items()):
            if total != 24:
                findings.append(Finding(
                    "layout-incomplete-row", label,
                    "panels at y=%d are %d/24 wide; a half-empty row reads as a "
                    "missing panel" % (y, total)))


def check_panel(panel, findings):
    title = panel.get("title", "?")
    kind = panel.get("type")
    defaults = panel.get("fieldConfig", {}).get("defaults", {})

    if not panel.get("description"):
        findings.append(Finding("panel-missing-description", title,
                                "no description: nothing tells the reader what "
                                "an empty panel means"))
    if kind != "text":
        if "unit" not in defaults:
            findings.append(Finding("panel-missing-unit", title, "no unit"))
        if "noValue" not in defaults:
            findings.append(Finding("panel-missing-novalue", title,
                                    "no noValue: absent data renders as blank, "
                                    "which reads the same as zero"))
        if panel.get("datasource", {}).get("uid") != DATASOURCE_REF:
            findings.append(Finding("panel-datasource-not-variable", title,
                                    "datasource is not the dashboard variable"))

    for target in panel.get("targets", []):
        ref = "%s/%s" % (title, target.get("refId", "?"))
        expr = target.get("expr", "")
        if target.get("datasource", {}).get("uid") != DATASOURCE_REF:
            findings.append(Finding("target-datasource-not-variable", ref,
                                    "target datasource is not the variable"))
        if expr.count("(") != expr.count(")") or expr.count("{") != expr.count("}"):
            findings.append(Finding("target-unbalanced", ref, "unbalanced brackets"))
        if HARDCODED_RANGE.search(expr):
            findings.append(Finding("target-hardcoded-range", ref,
                                    "hardcoded range window; use $__rate_interval "
                                    "or $__range so the panel follows the zoom"))
        if COUNTER_SELECTOR.search(expr) and not RATE_FN.search(expr):
            findings.append(Finding("target-counter-not-aggregated", ref,
                                    "counter selector outside rate/increase"))
        if "\\\\" in expr:
            findings.append(Finding("target-double-escaped", ref,
                                    "double-escaped backslash: PromQL will see a "
                                    "literal backslash and the match will never fire"))
        # A trailing `or vector(0)` is the whole query's fallback, which turns
        # "nothing scraped" into a confident zero. The same operator inside a
        # sum of two metric families only means an absent family contributes
        # nothing, and the enclosing expression still yields no data when both
        # are missing.
        if re.search(r"or\s+vector\(0\)\s*$", expr):
            findings.append(Finding("target-unwitnessed-zero", ref,
                                    "or vector(0) with nothing to prove the "
                                    "producer is alive turns absent into zero",
                                    severity="warn"))


def check_table(panel, findings):
    title = panel.get("title", "?")
    targets = panel.get("targets", [])
    transforms = {t["id"]: t.get("options", {}) for t in panel.get("transformations", [])}
    join = transforms.get("joinByField", {}).get("byField")
    organize = transforms.get("organize", {})

    if join and len(targets) == 1:
        findings.append(Finding("table-redundant-join", title,
                                "joining a single frame only reorders its fields"))

    if join and targets:
        first = targets[0]["expr"]
        labels = frame_labels(first)
        if join not in labels:
            findings.append(Finding("table-join-key-missing", title,
                                    "join field %r is not produced by the first "
                                    "query" % join))
        else:
            folded = joined_labels(first).get(join, set())
            extra = labels - {join} - folded
            if extra:
                findings.append(Finding(
                    "table-join-key-not-unique", title,
                    "rows are identified by %s but the join key is %r alone; "
                    "rows sharing it collapse into one carrying another's "
                    "numbers" % (sorted(labels), join)))

    fields, shown = simulate_columns(panel)
    index = organize.get("indexByName", {})
    if index:
        missing = [f for f in fields if f not in index]
        if missing:
            findings.append(Finding(
                "table-index-incomplete", title,
                "indexByName does not cover %s; Grafana drops the fields it "
                "cannot order" % missing))

    sort_by = (panel.get("options", {}).get("sortBy") or [{}])[0].get("displayName")
    if sort_by and sort_by not in shown:
        findings.append(Finding("table-sortby-missing-column", title,
                                "sortBy %r is not a rendered column" % sort_by))
    for override in panel.get("fieldConfig", {}).get("overrides", []):
        matcher = override.get("matcher", {})
        if matcher.get("id") == "byName" and matcher.get("options") not in shown:
            findings.append(Finding("table-override-missing-column", title,
                                    "override targets %r, which is not rendered"
                                    % matcher.get("options")))
    return shown


def lint(path, show_columns=False):
    with open(path) as handle:
        dashboard = json.load(handle)
    findings = []
    check_variables(dashboard, findings)
    check_layout(dashboard, findings)
    for panel in walk_panels(dashboard):
        check_panel(panel, findings)
        if panel.get("type") == "table":
            shown = check_table(panel, findings)
            if show_columns:
                print("  %-30s %s" % (panel.get("title", "?")[:30], shown))
    return findings


def main(argv):
    show_columns = "--columns" in argv
    paths = [a for a in argv if not a.startswith("--")]
    if not paths:
        paths = sorted(glob.glob(DEFAULT_GLOB))
    if not paths:
        print("no dashboards found under %s" % DEFAULT_GLOB, file=sys.stderr)
        return 1

    failed = 0
    for path in paths:
        findings = lint(path, show_columns)
        errors = [f for f in findings if f.severity == "error"]
        print("%s: %d error(s), %d warning(s)"
              % (os.path.relpath(path), len(errors), len(findings) - len(errors)))
        for finding in findings:
            print("  " + str(finding))
        failed += len(errors)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

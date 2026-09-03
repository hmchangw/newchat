#!/usr/bin/env python3
"""Run lint.py against the fixtures in testdata/.

Each fixture declares the rule ids it must produce in a top-level "_expect"
list. The comparison is exact in both directions: a rule that stops firing and
a rule that starts firing on something it should not are the same defect, and
an unverified rule can be disabled by an edit with nothing turning red.

    python3 tools/dashboards/lint_test.py
"""
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lint  # noqa: E402

TESTDATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata")


def main():
    fixtures = sorted(glob.glob(os.path.join(TESTDATA, "*.json")))
    if not fixtures:
        print("no fixtures in %s — a rule suite that tests nothing is the "
              "outcome this file exists to prevent" % TESTDATA, file=sys.stderr)
        return 1

    covered, failures = set(), 0
    for path in fixtures:
        name = os.path.basename(path)
        with open(path) as handle:
            expected = set(json.load(handle).get("_expect", []))
        actual = {f.rule for f in lint.lint(path)}
        covered |= actual
        if actual == expected:
            print("ok   %-32s %s" % (name, sorted(actual) or "(silent)"))
            continue
        failures += 1
        print("FAIL %s" % name)
        for rule in sorted(expected - actual):
            print("       expected but not raised: %s" % rule)
        for rule in sorted(actual - expected):
            print("       raised but not expected: %s" % rule)

    # A rule with no fixture can be disabled by an edit with nothing turning
    # red, so the id list is read back out of the linter and compared.
    with open(lint.__file__) as handle:
        known = set(re.findall(r'Finding\(\s*\n?\s*"([a-z][a-z0-9-]+)"',
                               handle.read()))
    uncovered = sorted(known - covered)
    if uncovered:
        failures += 1
        print("FAIL rules with no fixture: %s" % uncovered)

    print("\n%d fixture(s), %d rule(s) covered, %d failure(s)"
          % (len(fixtures), len(covered), failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

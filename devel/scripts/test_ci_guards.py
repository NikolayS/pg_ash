#!/usr/bin/env python3
"""Regression tests for mandatory-job and live-demo CI decisions."""

import unittest

import ci_guards


class RequiredJobsTests(unittest.TestCase):
    def test_both_required_jobs_succeeded(self):
        ci_guards.require_jobs({
            "docs-lint": {"result": "success"},
            "test": {"result": "success"},
        })

    def test_skipped_failed_cancelled_and_unknown_do_not_pass(self):
        for result in ("skipped", "failure", "cancelled", "pending", "", None):
            for job in ("docs-lint", "test"):
                with self.subTest(result=result, job=job):
                    needs = {name: {"result": "success"}
                             for name in ("docs-lint", "test")}
                    needs[job] = {"result": result}
                    with self.assertRaises(ValueError):
                        ci_guards.require_jobs(needs)

    def test_missing_and_malformed_evidence_do_not_pass(self):
        for needs in ({}, {"test": {"result": "success"}}, None, [],
                      {"docs-lint": {}, "test": {"result": "success"}}):
            with self.subTest(needs=needs), self.assertRaises(ValueError):
                ci_guards.require_jobs(needs)


class DemoDecisionTests(unittest.TestCase):
    def test_development_installer_and_discovery_trigger_real_capture(self):
        for path in ("devel/sql/ash-install.sql",
                     "devel/scripts/ash_sql_chain.py",
                     "demos/fixtures/shape.tsv", "sql/ash-install.sql",
                     "README.md", ".github/workflows/demo.yml",
                     "examples/llm-investigation.sql"):
            with self.subTest(path=path):
                self.assertTrue(ci_guards.demo_needed("pull_request", [path]))

    def test_manual_and_nightly_always_capture(self):
        for event in ("workflow_dispatch", "schedule"):
            self.assertTrue(ci_guards.demo_needed(event, []))

    def test_unrelated_changes_can_skip_expensive_capture(self):
        self.assertFalse(ci_guards.demo_needed("pull_request", ["LICENSE"]))

    def test_unknown_event_fails_closed(self):
        with self.assertRaises(ValueError):
            ci_guards.demo_needed("unexpected", [])


if __name__ == "__main__":
    unittest.main()

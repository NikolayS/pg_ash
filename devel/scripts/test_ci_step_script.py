#!/usr/bin/env python3
"""Unit tests for ci_step_script.py (stdlib only)."""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_step_script


SAMPLE = """\
name: sample
jobs:
  docs:
    steps:
      - name: Not the test job
        run: |
          exit 99
  test:
    steps:
      - uses: actions/checkout@example
      - name: First
        run: |
          printf '%s\\n' first

          echo done
      - name: "Second: quoted"
        run: |-
          echo second
      - name: 'Third''s step'
        run: |+
          echo third

"""


class WorkflowParserTests(unittest.TestCase):
    def parse(self, text: str = SAMPLE) -> list[ci_step_script.Step]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workflow.yml"
            path.write_text(text, encoding="utf-8")
            return ci_step_script.parse_workflow(path)

    def test_extracts_only_named_test_steps_and_honors_chomping(self) -> None:
        steps = self.parse()
        self.assertEqual(
            [step.name for step in steps],
            ["First", "Second: quoted", "Third's step"],
        )
        self.assertEqual(
            steps[0].run,
            "printf '%s\\n' first\n\n"
            "echo done\n",
        )
        self.assertEqual(steps[1].run, "echo second")
        self.assertEqual(steps[2].run, "echo third\n\n")

    def test_duplicate_workflow_names_fail_with_locations(self) -> None:
        text = SAMPLE.replace(
            "      - name: 'Third''s step'",
            "      - name: First",
        )
        with self.assertRaisesRegex(
            ci_step_script.WorkflowError, "duplicate step names.*First.*lines"
        ):
            self.parse(text)

    def test_unnamed_run_step_fails_instead_of_being_silently_omitted(self) -> None:
        text = SAMPLE.replace(
            "      - uses: actions/checkout@example",
            "      - run: |\n"
            "          echo unnamed",
        )
        with self.assertRaisesRegex(
            ci_step_script.WorkflowError, "executable step.*has no name"
        ):
            self.parse(text)

    def test_named_and_range_selection_are_strict(self) -> None:
        steps = self.parse()
        selected = ci_step_script.select_range(
            steps,
            "First",
            "Third's step",
            ["Second: quoted"],
        )
        self.assertEqual([step.name for step in selected], ["First", "Third's step"])

        with self.assertRaisesRegex(ci_step_script.WorkflowError, "unknown step"):
            ci_step_script.select_named(steps, ["missing"])
        with self.assertRaisesRegex(ci_step_script.WorkflowError, "occurs after"):
            ci_step_script.select_range(
                steps, "Third's step", "First", []
            )
        with self.assertRaisesRegex(ci_step_script.WorkflowError, "outside"):
            ci_step_script.select_range(
                steps, "Second: quoted", "Third's step", ["First"]
            )

    def test_run_mode_uses_a_fresh_bash_process_for_each_step(self) -> None:
        steps = [
            ci_step_script.Step(
                ordinal=1,
                line=1,
                name="set child environment",
                run="export CI_STEP_MUST_NOT_LEAK=yes\n",
            ),
            ci_step_script.Step(
                ordinal=2,
                line=2,
                name="check child environment",
                run='test -z "${CI_STEP_MUST_NOT_LEAK:-}"\n',
            ),
        ]
        args = argparse.Namespace(mode="run", env=[], cwd=None)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(ci_step_script._emit_or_run(args, steps), 0)

    def test_null_mode_preserves_exact_record_boundaries(self) -> None:
        steps = [
            ci_step_script.Step(
                ordinal=1,
                line=1,
                name="first",
                run="printf 'line one\\nline two\\n'\n",
            ),
            ci_step_script.Step(
                ordinal=2,
                line=2,
                name="second",
                run="echo done\n",
            ),
        ]
        args = argparse.Namespace(mode="null", env=[], cwd=None)

        class BinaryCapture(io.StringIO):
            def __init__(self) -> None:
                super().__init__()
                self.buffer = io.BytesIO()

        output = BinaryCapture()
        with contextlib.redirect_stdout(output):
            self.assertEqual(ci_step_script._emit_or_run(args, steps), 0)
        self.assertEqual(
            output.buffer.getvalue(),
            b"first\0printf 'line one\\nline two\\n'\n\0"
            b"second\0echo done\n\0",
        )

    def test_current_workflow_integration(self) -> None:
        steps = ci_step_script.parse_workflow(ci_step_script.DEFAULT_WORKFLOW)
        names = [step.name for step in steps]
        self.assertGreater(len(steps), 40)
        self.assertEqual(names[0], "Install pg_cron in container")
        self.assertIn("Test schema and infrastructure", names)
        self.assertIn("Degraded mode: without pg_stat_statements", names)
        self.assertNotIn("Guard SQL release workflow", names)
        selected = ci_step_script.select_named(
            steps, ["Test schema and infrastructure"]
        )[0]
        self.assertIsNotNone(selected.run)
        self.assertTrue((selected.run or "").startswith("psql -h localhost"))
        self.assertIn("Schema and infrastructure tests PASSED", selected.run or "")


if __name__ == "__main__":
    unittest.main()

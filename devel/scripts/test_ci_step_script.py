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

    # ---- issue #243: a dedented comment must end a run block ----

    # The workflow is fine; the parser was not. Before the fix the reader
    # skipped comment lines when looking for the end of a `run: |` block, so a
    # comment at job indentation was swallowed as block content, dragged the
    # computed content indentation down to its own, and raised. Any step that
    # is the last one in its job trips this the moment another job follows.
    DEDENTED_COMMENT = """\
jobs:
  test:
    steps:
      - name: Last step
        run: |
          echo hi
%s
  other-job:
    runs-on: ubuntu-latest
"""

    def test_dedented_comment_terminates_run_block(self) -> None:
        text = self.DEDENTED_COMMENT % (
            "  # One stable check name for the branch ruleset."
        )
        steps = self.parse(text)
        self.assertEqual([step.name for step in steps], ["Last step"])
        # Exact body: the comment must not leak in, and nothing may be trimmed.
        self.assertEqual(steps[0].run, "echo hi\n")

    def test_dedented_comment_and_no_comment_agree(self) -> None:
        """The comment is the only difference, so the body must be identical."""
        with_comment = self.parse(self.DEDENTED_COMMENT % "  # trailing note")
        without_comment = self.parse(self.DEDENTED_COMMENT % "")
        self.assertEqual(
            [step.run for step in with_comment],
            [step.run for step in without_comment],
        )

    def test_comment_indented_into_the_block_stays_content(self) -> None:
        """A shell comment inside `run: |` is content, not a terminator.

        This is the over-correction guard: terminating on any comment at all
        would silently truncate most real steps in this repository.
        """
        text = """\
jobs:
  test:
    steps:
      - name: Shell comments
        run: |
          # explain the next line
          echo hi
            # deeper still
          echo bye

  other-job:
    runs-on: ubuntu-latest
"""
        steps = self.parse(text)
        self.assertEqual(
            steps[0].run,
            "# explain the next line\necho hi\n  # deeper still\necho bye\n",
        )

    def test_blank_lines_inside_a_block_are_preserved(self) -> None:
        text = """\
jobs:
  test:
    steps:
      - name: Blanks
        run: |
          echo one

          echo two
  # dedented comment right after a blank-containing block
  other-job:
    runs-on: ubuntu-latest
"""
        steps = self.parse(text)
        self.assertEqual(steps[0].run, "echo one\n\necho two\n")

    def test_under_indented_content_is_not_newly_tolerated(self) -> None:
        """Characterization: the fix must not loosen anything for content.

        Note on issue #243's wording. It asks that "a genuinely under-indented
        content line still raises". Measured against the pre-fix parser, it
        never did: a non-comment line at or below the block's parent
        indentation has always simply ended the scalar, and the
        `content_indent <= parent_indent` guard was only ever reachable through
        the comment path -- which is the bug itself. Rather than invent a new
        error to satisfy the wording, this pins the pre-existing behaviour so a
        later change cannot quietly relax it: the block ends at the dedented
        line and that line's text never becomes part of the body.
        """
        text = """\
jobs:
  test:
    steps:
      - name: Bad indent
        run: |
          echo one
      echo two
  other-job:
    runs-on: ubuntu-latest
"""
        steps = self.parse(text)
        self.assertEqual(steps[0].run, "echo one\n")
        self.assertNotIn("echo two", steps[0].run or "")

    def test_tabs_are_still_rejected_when_ending_a_block(self) -> None:
        text = (
            "jobs:\n  test:\n    steps:\n      - name: Tabbed\n"
            "        run: |\n          echo hi\n\t# tabbed comment\n"
        )
        with self.assertRaises(ci_step_script.WorkflowError) as caught:
            self.parse(text)
        self.assertIn("tabs are not valid YAML indentation", str(caught.exception))

    def test_comment_below_content_indent_ends_literal_block(self) -> None:
        text = (
            "jobs:\n  test:\n    steps:\n      - name: Comment boundary\n"
            "        run: |\n          echo one\n"
            "         # YAML comment below the content indentation\n"
            "  other-job:\n    runs-on: ubuntu-latest\n"
        )
        steps = self.parse(text)
        self.assertEqual(steps[0].run, "echo one\n")

    def test_content_after_intermediate_comment_fails_loudly(self) -> None:
        text = (
            "jobs:\n  test:\n    steps:\n      - name: Bad boundary\n"
            "        run: |\n          echo one\n"
            "         # terminates scalar\n          echo two\n"
        )
        with self.assertRaisesRegex(ci_step_script.WorkflowError, "content resumes"):
            self.parse(text)

    def test_noncomment_below_content_indent_is_rejected(self) -> None:
        text = (
            "jobs:\n  test:\n    steps:\n      - name: Bad content indent\n"
            "        run: |\n          echo one\n         echo two\n"
        )
        with self.assertRaisesRegex(
            ci_step_script.WorkflowError, "line 7: inconsistent indentation"
        ):
            self.parse(text)

    def test_deeper_shell_content_keeps_its_indentation(self) -> None:
        text = (
            "jobs:\n  test:\n    steps:\n      - name: Shell indentation\n"
            "        run: |\n          if true; then\n"
            "            echo one\n          fi\n"
        )
        self.assertEqual(self.parse(text)[0].run, "if true; then\n  echo one\nfi\n")

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

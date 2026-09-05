#!/usr/bin/env python3
"""Select canonical Bash step bodies from the pg_ash CI workflow.

This intentionally implements only the small YAML subset used by
`.github/workflows/test.yml`: mapping keys, sequence entries, scalar step
names, and literal (`|`) run blocks.  Keeping the parser strict makes workflow
format drift fail loudly instead of silently extracting the wrong script.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORKFLOW = ROOT / ".github" / "workflows" / "test.yml"
ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
LITERAL_BLOCK_RE = re.compile(r"\|(?P<chomp>[+-]?)(?:\s+#.*)?\Z")


class WorkflowError(ValueError):
    """The workflow cannot be selected or parsed unambiguously."""


@dataclass(frozen=True)
class Step:
    """One named step in jobs.test."""

    ordinal: int
    line: int
    name: str
    run: str | None


def _without_eol(line: str) -> str:
    return line.rstrip("\r\n")


def _indent(line: str, *, line_number: int) -> int | None:
    """Return structural indentation, ignoring blank and comment-only lines."""
    indentation = _block_scalar_indent(line, line_number=line_number)
    if line.lstrip().startswith("#"):
        return None
    return indentation


def _block_scalar_indent(line: str, *, line_number: int) -> int | None:
    """Indentation for block-scalar termination; None only for blank lines.

    Unlike :func:`_indent`, a comment line reports its real indentation. Inside
    a literal block a ``#`` line indented at or beyond the content is ordinary
    content (a shell comment). The literal-block reader uses the first content
    line to recognize a later dedented YAML comment as a block terminator.
    """
    text = _without_eol(line)
    leading = text[: len(text) - len(text.lstrip(" \t"))]
    if "\t" in leading:
        raise WorkflowError(
            f"line {line_number}: tabs are not valid YAML indentation"
        )
    if not text.strip():
        return None
    return len(leading)


def _is_mapping_key(line: str, indent: int, key: str) -> bool:
    text = _without_eol(line)
    prefix = " " * indent + key + ":"
    if not text.startswith(prefix):
        return False
    remainder = text[len(prefix) :]
    return not remainder or remainder.isspace() or remainder.lstrip().startswith("#")


def _unique_key(
    lines: Sequence[str],
    *,
    start: int,
    end: int,
    indent: int,
    key: str,
    context: str,
) -> int:
    matches = [
        idx
        for idx in range(start, end)
        if _is_mapping_key(lines[idx], indent, key)
    ]
    if not matches:
        raise WorkflowError(f"{context}: missing {key!r} key")
    if len(matches) > 1:
        locations = ", ".join(str(idx + 1) for idx in matches)
        raise WorkflowError(
            f"{context}: duplicate {key!r} keys at lines {locations}"
        )
    return matches[0]


def _block_end(
    lines: Sequence[str], *, start: int, end: int, parent_indent: int
) -> int:
    for idx in range(start, end):
        indentation = _indent(lines[idx], line_number=idx + 1)
        if indentation is not None and indentation <= parent_indent:
            return idx
    return end


def _split_inline_comment(value: str) -> str:
    """Strip an unquoted YAML comment (` # ...`) from a plain scalar."""
    match = re.search(r"\s+#", value)
    return value[: match.start()] if match else value


def _parse_scalar(value: str, *, line_number: int, field: str) -> str:
    value = value.strip()
    if not value:
        raise WorkflowError(f"line {line_number}: empty {field}")

    if value.startswith('"'):
        try:
            parsed, offset = json.JSONDecoder().raw_decode(value)
        except json.JSONDecodeError as exc:
            raise WorkflowError(
                f"line {line_number}: invalid double-quoted {field}: {exc.msg}"
            ) from exc
        remainder = value[offset:].strip()
        if remainder and not remainder.startswith("#"):
            raise WorkflowError(
                f"line {line_number}: unexpected text after {field}: {remainder!r}"
            )
        if not isinstance(parsed, str):
            raise WorkflowError(f"line {line_number}: {field} must be a string")
        result = parsed
    elif value.startswith("'"):
        chars: list[str] = []
        idx = 1
        while idx < len(value):
            if value[idx] != "'":
                chars.append(value[idx])
                idx += 1
                continue
            if idx + 1 < len(value) and value[idx + 1] == "'":
                chars.append("'")
                idx += 2
                continue
            idx += 1
            remainder = value[idx:].strip()
            if remainder and not remainder.startswith("#"):
                raise WorkflowError(
                    f"line {line_number}: unexpected text after {field}: "
                    f"{remainder!r}"
                )
            break
        else:
            raise WorkflowError(
                f"line {line_number}: unterminated single-quoted {field}"
            )
        result = "".join(chars)
    else:
        result = _split_inline_comment(value).rstrip()

    if not result:
        raise WorkflowError(f"line {line_number}: empty {field}")
    if "\n" in result or "\r" in result or "\0" in result:
        raise WorkflowError(
            f"line {line_number}: {field} contains an unsupported control character"
        )
    return result


def _mapping_value(line: str, *, indent: int, key: str) -> str | None:
    text = _without_eol(line)
    prefix = " " * indent + key + ":"
    if not text.startswith(prefix):
        return None
    return text[len(prefix) :].strip()


def _sequence_mapping_value(line: str, *, indent: int, key: str) -> str | None:
    text = _without_eol(line)
    prefix = " " * indent + "- " + key + ":"
    if not text.startswith(prefix):
        return None
    return text[len(prefix) :].strip()


def _deindent_literal_block(
    lines: Sequence[str],
    *,
    start: int,
    end: int,
    parent_indent: int,
    chomp: str,
    run_line: int,
) -> str:
    block_end = end
    first_content_indent = None
    for idx in range(start, end):
        indentation = _block_scalar_indent(lines[idx], line_number=idx + 1)
        if indentation is None:
            continue
        if indentation <= parent_indent:
            block_end = idx
            break
        if first_content_indent is None:
            first_content_indent = indentation
        elif (indentation < first_content_indent
              and lines[idx].lstrip().startswith("#")):
            for later in range(idx + 1, end):
                later_indent = _block_scalar_indent(lines[later], line_number=later + 1)
                if later_indent is None or lines[later].lstrip().startswith("#"):
                    continue
                if later_indent <= parent_indent:
                    break
                raise WorkflowError(
                    f"line {later + 1}: content resumes after a dedented comment "
                    f"terminated the run block at line {run_line}"
                )
            block_end = idx
            break

    block_lines = list(lines[start:block_end])
    nonblank_indents = [
        len(_without_eol(line))
        - len(_without_eol(line).lstrip(" "))
        for line in block_lines
        if _without_eol(line).strip()
    ]
    if not nonblank_indents:
        return ""

    content_indent = min(nonblank_indents)
    if content_indent <= parent_indent:
        raise WorkflowError(
            f"line {run_line}: run block content must be indented more than "
            "the run key"
        )

    deindented: list[str] = []
    for offset, line in enumerate(block_lines, start=start + 1):
        text = _without_eol(line)
        newline = line[len(text) :]
        if not text.strip():
            deindented.append(newline)
            continue
        indentation = len(text) - len(text.lstrip(" "))
        if indentation < content_indent:
            raise WorkflowError(
                f"line {offset}: inconsistent indentation in run block "
                f"starting at line {run_line}"
            )
        deindented.append(text[content_indent:] + newline)

    body = "".join(deindented)
    if chomp == "+":
        return body
    stripped = body.rstrip("\r\n")
    if chomp == "-":
        return stripped
    return stripped + "\n" if body else ""


def _parse_step(
    lines: Sequence[str],
    *,
    start: int,
    end: int,
    step_indent: int,
    ordinal: int,
) -> Step | None:
    field_indent = step_indent + 2
    name_matches: list[tuple[int, str]] = []
    run_matches: list[tuple[int, str]] = []
    inline_name = _sequence_mapping_value(
        lines[start], indent=step_indent, key="name"
    )
    if inline_name is not None:
        name_matches.append((start, inline_name))
    inline_run = _sequence_mapping_value(
        lines[start], indent=step_indent, key="run"
    )
    if inline_run is not None:
        run_matches.append((start, inline_run))
    for idx in range(start + 1, end):
        value = _mapping_value(lines[idx], indent=field_indent, key="name")
        if value is not None:
            name_matches.append((idx, value))
        value = _mapping_value(lines[idx], indent=field_indent, key="run")
        if value is not None:
            run_matches.append((idx, value))

    # `uses:`-only steps may legitimately be unnamed and are not selectable,
    # but silently omitting an executable step would weaken a selected range.
    if not name_matches:
        if run_matches:
            locations = ", ".join(str(idx + 1) for idx, _ in run_matches)
            raise WorkflowError(
                f"executable step at line {start + 1} has no name "
                f"(run key at line(s) {locations})"
            )
        return None
    if len(name_matches) > 1:
        locations = ", ".join(str(idx + 1) for idx, _ in name_matches)
        raise WorkflowError(
            f"step at line {start + 1}: duplicate name keys at lines {locations}"
        )
    name_line, name_value = name_matches[0]
    name = _parse_scalar(name_value, line_number=name_line + 1, field="step name")

    if len(run_matches) > 1:
        locations = ", ".join(str(idx + 1) for idx, _ in run_matches)
        raise WorkflowError(
            f"step {name!r}: duplicate run keys at lines {locations}"
        )
    if not run_matches:
        return Step(ordinal=ordinal, line=name_line + 1, name=name, run=None)

    run_idx, indicator = run_matches[0]
    match = LITERAL_BLOCK_RE.fullmatch(indicator)
    if not match:
        raise WorkflowError(
            f"line {run_idx + 1}: step {name!r} must use a literal run block "
            f"(`run: |`, `|-`, or `|+`), got {indicator!r}"
        )
    body = _deindent_literal_block(
        lines,
        start=run_idx + 1,
        end=end,
        parent_indent=field_indent,
        chomp=match.group("chomp"),
        run_line=run_idx + 1,
    )
    return Step(ordinal=ordinal, line=name_line + 1, name=name, run=body)


def parse_workflow(path: Path, *, job: str = "test") -> list[Step]:
    """Parse and validate all named steps under jobs.<job>.steps."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    except OSError as exc:
        raise WorkflowError(f"cannot read workflow {path}: {exc}") from exc

    jobs_idx = _unique_key(
        lines,
        start=0,
        end=len(lines),
        indent=0,
        key="jobs",
        context=str(path),
    )
    jobs_end = _block_end(
        lines, start=jobs_idx + 1, end=len(lines), parent_indent=0
    )
    job_idx = _unique_key(
        lines,
        start=jobs_idx + 1,
        end=jobs_end,
        indent=2,
        key=job,
        context=f"{path}: jobs",
    )
    job_end = _block_end(lines, start=job_idx + 1, end=jobs_end, parent_indent=2)
    steps_idx = _unique_key(
        lines,
        start=job_idx + 1,
        end=job_end,
        indent=4,
        key="steps",
        context=f"{path}: jobs.{job}",
    )

    step_indent = 6
    starts: list[int] = []
    for idx in range(steps_idx + 1, job_end):
        indentation = _indent(lines[idx], line_number=idx + 1)
        if indentation != step_indent:
            continue
        text = _without_eol(lines[idx])[step_indent:]
        if text == "-" or text.startswith("- "):
            starts.append(idx)

    if not starts:
        raise WorkflowError(f"{path}: jobs.{job}.steps has no sequence entries")

    parsed: list[Step] = []
    for offset, start in enumerate(starts):
        end = starts[offset + 1] if offset + 1 < len(starts) else job_end
        step = _parse_step(
            lines,
            start=start,
            end=end,
            step_indent=step_indent,
            ordinal=offset + 1,
        )
        if step is not None:
            parsed.append(step)

    by_name: dict[str, list[Step]] = {}
    for step in parsed:
        by_name.setdefault(step.name, []).append(step)
    duplicates = {name: values for name, values in by_name.items() if len(values) > 1}
    if duplicates:
        details = "; ".join(
            f"{name!r} at lines {', '.join(str(step.line) for step in values)}"
            for name, values in sorted(duplicates.items())
        )
        raise WorkflowError(f"duplicate step names in jobs.{job}: {details}")
    return parsed


def _step_map(steps: Sequence[Step]) -> dict[str, Step]:
    return {step.name: step for step in steps}


def select_named(steps: Sequence[Step], names: Sequence[str]) -> list[Step]:
    repeated = sorted(name for name, count in Counter(names).items() if count > 1)
    if repeated:
        raise WorkflowError(
            "step requested more than once: "
            + ", ".join(repr(name) for name in repeated)
        )
    available = _step_map(steps)
    missing = [name for name in names if name not in available]
    if missing:
        raise WorkflowError(
            "unknown step name(s): " + ", ".join(repr(name) for name in missing)
        )
    return [available[name] for name in names]


def select_range(
    steps: Sequence[Step],
    start_name: str,
    end_name: str,
    excludes: Sequence[str],
) -> list[Step]:
    available = _step_map(steps)
    missing_endpoints = [
        name for name in (start_name, end_name) if name not in available
    ]
    if missing_endpoints:
        raise WorkflowError(
            "unknown range endpoint(s): "
            + ", ".join(repr(name) for name in missing_endpoints)
        )

    repeated = sorted(
        name for name, count in Counter(excludes).items() if count > 1
    )
    if repeated:
        raise WorkflowError(
            "step excluded more than once: "
            + ", ".join(repr(name) for name in repeated)
        )

    positions = {step.name: idx for idx, step in enumerate(steps)}
    start = positions[start_name]
    end = positions[end_name]
    if start > end:
        raise WorkflowError(
            f"range start {start_name!r} occurs after end {end_name!r}"
        )

    unknown_excludes = [name for name in excludes if name not in available]
    if unknown_excludes:
        raise WorkflowError(
            "unknown excluded step(s): "
            + ", ".join(repr(name) for name in unknown_excludes)
        )
    outside = [
        name for name in excludes if not start <= positions[name] <= end
    ]
    if outside:
        raise WorkflowError(
            "excluded step(s) outside selected range: "
            + ", ".join(repr(name) for name in outside)
        )

    excluded = set(excludes)
    return [step for step in steps[start : end + 1] if step.name not in excluded]


def _require_run_bodies(steps: Sequence[Step]) -> None:
    without_run = [step.name for step in steps if step.run is None]
    if without_run:
        raise WorkflowError(
            "selected step(s) have no run block: "
            + ", ".join(repr(name) for name in without_run)
        )


def _parse_env(assignments: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for assignment in assignments:
        if "=" not in assignment:
            raise WorkflowError(
                f"invalid --env {assignment!r}; expected NAME=VALUE"
            )
        name, value = assignment.split("=", 1)
        if not ENV_NAME_RE.fullmatch(name):
            raise WorkflowError(f"invalid environment variable name {name!r}")
        if name in result:
            raise WorkflowError(f"environment variable {name!r} set more than once")
        result[name] = value
    return result


def _emit_or_run(args: argparse.Namespace, steps: Sequence[Step]) -> int:
    _require_run_bodies(steps)
    mode = args.mode

    if args.env and mode != "run":
        raise WorkflowError("--env is only valid with --run")
    if args.cwd is not None and mode != "run":
        raise WorkflowError("--cwd is only valid with --run")

    if mode == "names":
        for step in steps:
            print(step.name)
        return 0

    if mode == "json":
        json.dump(
            [
                {
                    "ordinal": step.ordinal,
                    "line": step.line,
                    "name": step.name,
                    "run": step.run,
                }
                for step in steps
            ],
            sys.stdout,
            ensure_ascii=False,
            indent=2,
        )
        sys.stdout.write("\n")
        return 0

    if mode == "null":
        output = sys.stdout.buffer
        for step in steps:
            output.write(step.name.encode("utf-8"))
            output.write(b"\0")
            output.write((step.run or "").encode("utf-8"))
            output.write(b"\0")
        return 0

    if mode == "run":
        bash = shutil.which("bash")
        if bash is None:
            raise WorkflowError("cannot execute steps: bash is not on PATH")
        environment = os.environ.copy()
        environment.update(_parse_env(args.env))
        cwd = args.cwd.resolve() if args.cwd is not None else None
        for position, step in enumerate(steps, start=1):
            print(
                f"[ci-step {position}/{len(steps)}] {step.name}",
                file=sys.stderr,
                flush=True,
            )
            completed = subprocess.run(
                [
                    bash,
                    "--noprofile",
                    "--norc",
                    "-e",
                    "-o",
                    "pipefail",
                    "-c",
                    step.run or "",
                    "ci-step",
                ],
                cwd=cwd,
                env=environment,
                check=False,
            )
            if completed.returncode != 0:
                print(
                    f"[ci-step FAILED rc={completed.returncode}] {step.name}",
                    file=sys.stderr,
                )
                return completed.returncode
        return 0

    if len(steps) != 1:
        raise WorkflowError(
            f"raw body output selected {len(steps)} steps; use --names-only "
            "and request each step separately, --null, --json, or --run so "
            "step boundaries remain unambiguous"
        )
    sys.stdout.write(steps[0].run or "")
    return 0


def _add_selection_output_options(parser: argparse.ArgumentParser) -> None:
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--names-only",
        dest="mode",
        action="store_const",
        const="names",
        help="emit selected names, one per line",
    )
    modes.add_argument(
        "--json",
        dest="mode",
        action="store_const",
        const="json",
        help="emit selected step metadata and exact bodies as JSON",
    )
    modes.add_argument(
        "--null",
        dest="mode",
        action="store_const",
        const="null",
        help="emit repeating NAME NUL BODY NUL records",
    )
    modes.add_argument(
        "--run",
        dest="mode",
        action="store_const",
        const="run",
        help=(
            "run each selected body in a fresh GitHub-style Bash process; "
            "workflow `if` and `env` fields are not evaluated"
        ),
    )
    parser.set_defaults(mode="body")
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="environment override for --run; may be repeated",
    )
    parser.add_argument(
        "--cwd",
        type=Path,
        help="working directory for --run (default: inherit current directory)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Extract exact Bash run blocks from named jobs.test workflow steps."
        )
    )
    parser.add_argument(
        "--workflow",
        type=Path,
        default=DEFAULT_WORKFLOW,
        help=f"workflow file (default: {DEFAULT_WORKFLOW})",
    )
    parser.add_argument(
        "--job",
        default="test",
        help="job under jobs to inspect (default: test)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    list_parser = commands.add_parser("list", help="list named steps")
    list_parser.add_argument(
        "--long",
        action="store_true",
        help="include sequence ordinal, source line, and run/uses kind",
    )

    step_parser = commands.add_parser(
        "step", help="select one or more exact step names"
    )
    step_parser.add_argument("names", nargs="+", metavar="NAME")
    _add_selection_output_options(step_parser)

    range_parser = commands.add_parser(
        "range", help="select an inclusive range in workflow order"
    )
    range_parser.add_argument("start", metavar="START")
    range_parser.add_argument("end", metavar="END")
    range_parser.add_argument(
        "--exclude",
        action="append",
        nargs="+",
        default=[],
        metavar="NAME",
        help="exact step name(s) to omit; may be repeated",
    )
    _add_selection_output_options(range_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        steps = parse_workflow(args.workflow.resolve(), job=args.job)
        if args.command == "list":
            for step in steps:
                if args.long:
                    kind = "run" if step.run is not None else "no-run"
                    print(f"{step.ordinal}\t{step.line}\t{kind}\t{step.name}")
                else:
                    print(step.name)
            return 0

        if args.command == "step":
            selected = select_named(steps, args.names)
        else:
            excludes = [name for group in args.exclude for name in group]
            selected = select_range(steps, args.start, args.end, excludes)
        return _emit_or_run(args, selected)
    except WorkflowError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

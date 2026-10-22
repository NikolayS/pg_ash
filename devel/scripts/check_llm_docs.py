#!/usr/bin/env python3
"""Guard the example links and report vocabulary; SQL tests verify semantics."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    readme = (ROOT / "README.md").read_text()
    prompt = (ROOT / "examples/llm-prompt.md").read_text()
    example = (ROOT / "examples/llm-investigation.sql").read_text()
    for name in ("llm-investigation.sql", "llm-prompt.md"):
        assert f"examples/{name}" in readme, f"README must link {name}"
    for field in (
        "aas_avg", "aas_worst1m", "aas_p99", "aas_p999", "top_events_*",
        "top_queryids_available", "coverage", "minutes_with_data",
        "minutes_expected", "raw_retention_start", "vcpus", "include_bg_workers",
    ):
        assert field in prompt, f"analysis prompt must explain {field}"
    for step in range(1, 6):
        assert f"Step {step}:" in example, f"missing executable step {step}"
    assert "read only;" in example, "example must open a read-only transaction"
    assert "rollback;" in example, "example must finish its transaction"
    print("LLM documentation links and vocabulary PASSED")


if __name__ == "__main__":
    main()

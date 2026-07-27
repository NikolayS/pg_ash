#!/usr/bin/env python3
"""Regression tests for release-tag/payload identity enforcement."""

from __future__ import annotations

import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ash_sql_chain


CHECKER = Path(__file__).with_name("check_release_stamp.py")


class ReleaseStampTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.payload_path = ash_sql_chain.ROOT / "sql" / "ash-install.sql"
        cls.payload_version = ash_sql_chain.install_version(cls.payload_path)
        core_parts = cls.payload_version.split("-", 1)[0].split(".")
        cls.final_version = ".".join(core_parts + ["0"] * (3 - len(core_parts)))
        cls.prerelease_version = f"{cls.final_version}-rc.1"

    def run_checker(
        self,
        tag: str,
        payload: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = [sys.executable, str(CHECKER), "--tag", tag]
        if payload is not None:
            command.extend(["--payload", str(payload)])
        return subprocess.run(command, capture_output=True, text=True, check=False)

    def stamped_payload(self, directory: str, version: str) -> Path:
        payload = Path(directory) / "ash-install.sql"
        payload.write_text(
            self.payload_path.read_text().replace(
                self.payload_version,
                version,
            )
        )
        return payload

    def test_mismatched_tag_rejects_payload(self) -> None:
        mismatched_version = (
            self.final_version
            if self.payload_version != self.final_version
            else self.prerelease_version
        )
        tag = f"v{mismatched_version}"
        result = self.run_checker(tag)

        self.assertNotEqual(
            result.returncode,
            0,
            (
                f"checker accepted release tag {tag} for payload "
                f"ash.config.version={self.payload_version}"
            ),
        )
        self.assertIn(tag, result.stderr)
        self.assertIn(self.payload_version, result.stderr)

    def test_matching_prerelease_stamps_pass(self) -> None:
        for stage in ("dev", "alpha", "beta", "rc"):
            with self.subTest(stage=stage):
                version = f"{self.final_version}-{stage}.1"
                with tempfile.TemporaryDirectory() as temp_dir:
                    payload = self.stamped_payload(temp_dir, version)
                    tag = f"v{version}"
                    result = self.run_checker(tag, payload)

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(tag, result.stdout)
                self.assertIn(version, result.stdout)

    def test_matching_final_stamp_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            final_payload = self.stamped_payload(temp_dir, self.final_version)
            tag = f"v{self.final_version}"
            result = self.run_checker(tag, final_payload)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(tag, result.stdout)
        self.assertIn(self.final_version, result.stdout)

    def test_matching_nonstandard_tag_is_rejected(self) -> None:
        nonstandard_version = f"{self.final_version}-preview.1"
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, nonstandard_version)
            result = self.run_checker(f"v{nonstandard_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use vX.Y.Z", result.stderr)

    def test_numeric_identifiers_reject_leading_zeroes(self) -> None:
        leading_zero_version = f"0{self.final_version}"
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, leading_zero_version)
            result = self.run_checker(f"v{leading_zero_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use vX.Y.Z", result.stderr)

    def test_inconsistent_payload_stamps_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, self.final_version)
            update_stamp = (
                f"update ash.config set version = '{self.final_version}'"
            )
            inconsistent_update_stamp = (
                f"update ash.config set version = '{self.prerelease_version}'"
            )
            payload_text = payload.read_text()
            self.assertIn(update_stamp, payload_text)
            payload.write_text(
                payload_text.replace(
                    update_stamp,
                    inconsistent_update_stamp,
                    1,
                )
            )
            result = self.run_checker(f"v{self.final_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inconsistent ash.config version stamps", result.stderr)

    def test_tag_requires_v_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, self.final_version)
            result = self.run_checker(self.final_version, payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must start with 'v'", result.stderr)


class SQLChainOverlayTest(unittest.TestCase):
    def test_prerelease_development_installer_extends_upgrade_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.0.sql").write_text("-- released installer\n")
            (released_migrations / "ash-1.0-to-1.1.sql").write_text(
                "-- released migration\n"
            )
            (development_sql / "ash-install.sql").write_text(
                "-- development installer\n"
            )

            full_output = io.StringIO()
            reapply_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
            ):
                with contextlib.redirect_stdout(full_output):
                    ash_sql_chain.emit_full_upgrade_chain("1.0")
                with contextlib.redirect_stdout(reapply_output):
                    ash_sql_chain.emit_reapply_chain()

        self.assertEqual(
            full_output.getvalue().splitlines(),
            [
                r"\i sql/ash-1.0.sql",
                r"\i sql/migrations/ash-1.0-to-1.1.sql",
                r"\i devel/sql/ash-install.sql",
            ],
        )
        self.assertEqual(
            reapply_output.getvalue().splitlines(),
            [r"\i devel/sql/ash-install.sql"],
        )

    def test_connected_development_migration_owns_the_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.0.sql").write_text("-- released installer\n")
            (released_migrations / "ash-1.0-to-1.1.sql").write_text(
                "-- released migration\n"
            )
            (development_sql / "ash-1.1-to-1.2.sql").write_text(
                "-- development migration includes ash-install.sql\n"
            )
            (development_sql / "ash-install.sql").write_text(
                "-- development installer\n"
            )

            full_output = io.StringIO()
            reapply_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
            ):
                with contextlib.redirect_stdout(full_output):
                    ash_sql_chain.emit_full_upgrade_chain("1.0")
                with contextlib.redirect_stdout(reapply_output):
                    ash_sql_chain.emit_reapply_chain()

        expected_migrations = [
            r"\i sql/ash-1.0.sql",
            r"\i sql/migrations/ash-1.0-to-1.1.sql",
            r"\i devel/sql/ash-1.1-to-1.2.sql",
        ]
        self.assertEqual(full_output.getvalue().splitlines(), expected_migrations)
        self.assertEqual(
            reapply_output.getvalue().splitlines(),
            [r"\i devel/sql/ash-1.1-to-1.2.sql"],
        )


if __name__ == "__main__":
    unittest.main()

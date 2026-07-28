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
        cls.final_version = cls.payload_version.split("-", 1)[0]
        cls.prerelease_version = f"{cls.final_version}-rc1"

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
        for stage in ("alpha", "beta", "rc"):
            with self.subTest(stage=stage):
                version = f"{self.final_version}-{stage}1"
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

    def test_existing_prerelease_payload_tag_passes(self) -> None:
        tag = f"v{self.payload_version}"
        result = self.run_checker(tag)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(tag, result.stdout)
        self.assertIn(self.payload_version, result.stdout)

    def test_matching_nonstandard_tag_is_rejected(self) -> None:
        nonstandard_version = f"{self.final_version}-preview1"
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, nonstandard_version)
            result = self.run_checker(f"v{nonstandard_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use vX.Y", result.stderr)

    def test_three_part_tag_is_rejected(self) -> None:
        three_part_version = f"{self.final_version}.0"
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, three_part_version)
            result = self.run_checker(f"v{three_part_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use vX.Y", result.stderr)

    def test_numeric_identifiers_reject_leading_zeroes(self) -> None:
        leading_zero_version = f"0{self.final_version}"
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = self.stamped_payload(temp_dir, leading_zero_version)
            result = self.run_checker(f"v{leading_zero_version}", payload)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use vX.Y", result.stderr)

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
    @staticmethod
    def write_stamped_installer(path: Path, version: str) -> None:
        path.write_text(
            "create table ash.config (\n"
            f"  version text not null default '{version}'\n"
            ");\n"
            f"update ash.config set version = '{version}' where singleton;\n"
            "alter table ash.config alter column version "
            f"set default '{version}';\n"
        )

    def test_prerelease_development_installer_extends_upgrade_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.0.sql").write_text("-- released installer\n")
            self.write_stamped_installer(
                root / "sql" / "ash-install.sql",
                "1.1-beta1",
            )
            (released_migrations / "ash-1.0-to-1.1.sql").write_text(
                "-- released migration\n"
            )
            self.write_stamped_installer(
                development_sql / "ash-install.sql",
                "1.1",
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

    def test_next_release_line_installer_requires_migration(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.5.sql").write_text(
                "-- released installer\n"
            )
            self.write_stamped_installer(
                root / "sql" / "ash-install.sql",
                "2.0",
            )
            (released_migrations / "ash-1.5-to-2.0.sql").write_text(
                "-- released migration\n"
            )
            self.write_stamped_installer(
                development_sql / "ash-install.sql",
                "2.1",
            )

            full_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
                contextlib.redirect_stdout(full_output),
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    (
                        r"^development installer targets release line 2\.1, "
                        r"but the upgrade graph stops at 2\.0; add a connected "
                        r"development migration$"
                    ),
                ):
                    ash_sql_chain.emit_full_upgrade_chain("1.5")

        self.assertEqual(full_output.getvalue(), "")

    def test_final_release_requires_migration_for_same_line_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.5.sql").write_text(
                "-- released installer\n"
            )
            self.write_stamped_installer(
                root / "sql" / "ash-install.sql",
                "2.0",
            )
            (released_migrations / "ash-1.5-to-2.0.sql").write_text(
                "-- released migration\n"
            )
            self.write_stamped_installer(
                development_sql / "ash-install.sql",
                "2.0",
            )

            full_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
                contextlib.redirect_stdout(full_output),
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    (
                        r"^a lone development installer is valid only after "
                        r"a prerelease; the released payload 2\.0 is final, "
                        r"so add a connected development migration$"
                    ),
                ):
                    ash_sql_chain.emit_full_upgrade_chain("1.5")

        self.assertEqual(full_output.getvalue(), "")

    def test_pinned_upgrade_chain_applies_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.5.sql").write_text(
                "-- released installer\n"
            )
            released_installer = root / "sql" / "ash-install.sql"
            self.write_stamped_installer(released_installer, "2.0-beta1")
            (released_migrations / "ash-1.5-to-2.0.sql").write_text(
                "-- released migration\n"
            )
            (root / "sql" / "ash-1.5-to-2.0.sql").write_text(
                r"\ir migrations/ash-1.5-to-2.0.sql" "\n"
            )
            development_installer = development_sql / "ash-install.sql"
            self.write_stamped_installer(development_installer, "2.0")

            pinned_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
            ):
                released_version = ash_sql_chain.install_version(
                    released_installer
                )
                fresh_version = ash_sql_chain.fresh_install_version()
                with contextlib.redirect_stdout(pinned_output):
                    ash_sql_chain.emit_pinned_upgrade_chain("1.5")
                pinned_paths = [
                    root / line.removeprefix(r"\i ")
                    for line in pinned_output.getvalue().splitlines()
                ]
                pinned_version = ash_sql_chain.install_version(pinned_paths[-1])

        self.assertEqual(released_version, "2.0-beta1")
        self.assertEqual(fresh_version, "2.0")
        self.assertEqual(
            pinned_output.getvalue().splitlines(),
            [
                r"\i sql/ash-1.5-to-2.0.sql",
                r"\i devel/sql/ash-install.sql",
            ],
        )
        self.assertEqual(pinned_version, fresh_version)

    def test_disconnected_released_chain_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.0.sql").write_text(
                "-- released installer\n"
            )
            (released_migrations / "ash-1.0-to-1.1.sql").write_text(
                "-- released migration\n"
            )
            (released_migrations / "ash-1.2-to-1.3.sql").write_text(
                "-- detached released migration\n"
            )
            (development_sql / "ash-install.sql").write_text(
                "-- development installer\n"
            )

            full_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
                contextlib.redirect_stdout(full_output),
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    (
                        r"^disconnected released upgrade chain from 1\.0: "
                        r"stopped at 1\.1, expected to reach 1\.3$"
                    ),
                ):
                    ash_sql_chain.emit_full_upgrade_chain("1.0")

        self.assertEqual(full_output.getvalue(), "")

    def test_detached_released_edge_below_head_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            released_migrations = root / "sql" / "migrations"
            development_sql = root / "devel" / "sql"
            released_migrations.mkdir(parents=True)
            development_sql.mkdir(parents=True)
            (root / "sql" / "ash-1.0.sql").write_text(
                "-- released installer\n"
            )
            (released_migrations / "ash-1.0-to-2.0.sql").write_text(
                "-- released migration\n"
            )
            (released_migrations / "ash-1.2-to-1.3.sql").write_text(
                "-- detached released migration below the head\n"
            )
            (development_sql / "ash-install.sql").write_text(
                "-- development installer\n"
            )

            full_output = io.StringIO()
            with (
                mock.patch.object(ash_sql_chain, "ROOT", root),
                mock.patch.object(
                    ash_sql_chain,
                    "UPGRADE_DIRS",
                    (released_migrations, development_sql),
                ),
                contextlib.redirect_stdout(full_output),
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    (
                        r"^disconnected released upgrade graph: not reachable "
                        r"from a released installer: "
                        r"sql/migrations/ash-1\.2-to-1\.3\.sql$"
                    ),
                ):
                    ash_sql_chain.emit_full_upgrade_chain("1.0")

        self.assertEqual(full_output.getvalue(), "")


if __name__ == "__main__":
    unittest.main()

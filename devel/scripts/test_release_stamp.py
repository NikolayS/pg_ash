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
            self.write_stamped_installer(
                root / "sql" / "ash-install.sql",
                "1.1",
            )
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

    def test_final_release_requires_migration_for_changed_same_line_overlay(self) -> None:
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

            with (development_sql / "ash-install.sql").open("a") as output:
                output.write("select 1;\n")

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
                        r"^the released payload 2\.0 is final, but the "
                        r"development installer is not byte-identical; add a "
                        r"connected development migration$"
                    ),
                ):
                    ash_sql_chain.emit_full_upgrade_chain("1.5")

        self.assertEqual(full_output.getvalue(), "")

    def test_promoted_installer_requires_released_migration(self) -> None:
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
                "2.1",
            )
            (released_migrations / "ash-1.5-to-2.0.sql").write_text(
                "-- released migration\n"
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
                        r"^released installer targets release line 2\.1, "
                        r"but the released upgrade graph stops at 2\.0; "
                        r"add or promote a connected migration$"
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


class FinalBaselineOverlayTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.released = self.root / "sql" / "migrations"
        self.dev = self.root / "devel" / "sql"
        self.released.mkdir(parents=True)
        self.dev.mkdir(parents=True)
        (self.root / "sql" / "ash-1.0.sql").write_text("-- historical installer\n")
        for name in ("ash-1.0-to-1.5.sql", "ash-1.5-to-2.0.sql"):
            (self.released / name).write_text("-- released migration\n")
            (self.root / "sql" / name).write_text(r"\ir migrations/" + name + "\n")
        self.release_install = self.root / "sql" / "ash-install.sql"
        self.dev_install = self.dev / "ash-install.sql"
        SQLChainOverlayTest.write_stamped_installer(self.release_install, "2.0")
        self.dev_install.write_bytes(self.release_install.read_bytes())
        for name, value in (
            ("ROOT", self.root),
            ("UPGRADE_DIRS", (self.released, self.dev)),
        ):
            patcher = mock.patch.object(ash_sql_chain, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.commands = (
            (ash_sql_chain.emit_full_upgrade_chain, ("1.0",)),
            (ash_sql_chain.emit_pinned_upgrade_chain, ("1.5",)),
            (ash_sql_chain.emit_reapply_chain, ()),
        )

    def assert_rejected(self, diagnostic: str) -> None:
        for command, args in self.commands:
            with self.subTest(command=command.__name__):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    with self.assertRaisesRegex(SystemExit, diagnostic):
                        command(*args)
                self.assertEqual(output.getvalue(), "")

    def test_final_baseline_extends_all_upgrade_paths_exactly_once(self) -> None:
        expected = (
            [r"\i sql/ash-1.0.sql",
             r"\i sql/migrations/ash-1.0-to-1.5.sql",
             r"\i sql/migrations/ash-1.5-to-2.0.sql",
             r"\i devel/sql/ash-install.sql"],
            [r"\i sql/ash-1.5-to-2.0.sql", r"\i devel/sql/ash-install.sql"],
            [r"\i devel/sql/ash-install.sql"],
        )
        for (command, args), lines in zip(self.commands, expected):
            with self.subTest(command=command.__name__):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    command(*args)
                self.assertEqual(output.getvalue().splitlines(), lines)

    def test_any_changed_final_baseline_requires_migration(self) -> None:
        original = self.release_install.read_bytes()
        changes = {
            "sql": original + b"select 1;\n",
            "comment": original + b"-- changed\n",
            "line_endings": original.replace(b"\n", b"\r\n"),
            "same_line_stamp": original.replace(b"'2.0'", b"'2.0-rc1'"),
        }
        for label, payload in changes.items():
            with self.subTest(change=label):
                self.dev_install.write_bytes(payload)
                self.assert_rejected("connected development migration")

    def test_baseline_does_not_hide_disconnected_released_chain(self) -> None:
        (self.released / "ash-1.0-to-1.5.sql").unlink()
        self.assert_rejected("disconnected released upgrade chain")

    def test_baseline_does_not_hide_detached_development_migration(self) -> None:
        (self.dev / "ash-1.1-to-1.2.sql").write_text("-- disconnected\n")
        self.assert_rejected("disconnected .*graph")

    def test_identical_inconsistent_stamps_are_rejected(self) -> None:
        payload = self.release_install.read_bytes().replace(
            b"update ash.config set version = '2.0'",
            b"update ash.config set version = '2.0-rc1'",
        )
        self.release_install.write_bytes(payload)
        self.dev_install.write_bytes(payload)
        self.assert_rejected("inconsistent ash.config version stamps")

    def test_inconsistent_development_stamps_are_rejected(self) -> None:
        self.dev_install.write_bytes(self.dev_install.read_bytes().replace(
            b"update ash.config set version = '2.0'",
            b"update ash.config set version = '2.0-rc1'",
        ))
        self.assert_rejected("inconsistent ash.config version stamps")

    def test_baseline_does_not_allow_finalized_migration_override(self) -> None:
        (self.dev / "ash-1.5-to-2.0.sql").write_text(r"\ir ash-install.sql")
        self.assert_rejected("duplicate upgrade")

    def test_identical_payload_cannot_hide_missing_released_migration(self) -> None:
        SQLChainOverlayTest.write_stamped_installer(self.release_install, "2.1")
        self.dev_install.write_bytes(self.release_install.read_bytes())
        self.assert_rejected("released upgrade graph stops at 2.0")


if __name__ == "__main__":
    unittest.main()

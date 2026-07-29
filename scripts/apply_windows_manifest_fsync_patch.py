from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


script_path = Path("scripts/build_target_acceptance_manifest.py")
script = script_path.read_text(encoding="utf-8")
script = replace_once(
    script,
    '''def _stage_summaries(
''',
    '''def _fsync_copied_file(path: Path) -> None:
    # Windows os.fsync() delegates to _commit(), which rejects a read-only
    # descriptor with EBADF. The copied file is already closed by copyfile;
    # reopen it read/write solely to commit its contents durably.
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _stage_summaries(
''',
    label="fsync helper insertion",
)
script = replace_once(
    script,
    '''            shutil.copyfile(source, target)
            with target.open("rb") as handle:
                os.fsync(handle.fileno())
            staged[gate] = target
''',
    '''            shutil.copyfile(source, target)
            _fsync_copied_file(target)
            staged[gate] = target
''',
    label="staging fsync call",
)
script_path.write_text(script, encoding="utf-8")


test_path = Path("tester/target_cutover_preflight_tester.py")
test = test_path.read_text(encoding="utf-8")
test = replace_once(
    test,
    '''import unittest
from pathlib import Path
''',
    '''import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
''',
    label="mock imports",
)
test = replace_once(
    test,
    '''from ibmd.foundation.atomic_json import atomic_write_json
''',
    '''from scripts.build_target_acceptance_manifest import _fsync_copied_file

from ibmd.foundation.atomic_json import atomic_write_json
''',
    label="fsync helper test import",
)
test = replace_once(
    test,
    '''    def test_manifest_roundtrip_and_tamper_detection(self) -> None:
''',
    '''    def test_copied_evidence_fsync_uses_writable_descriptor(self) -> None:
        handle = MagicMock()
        handle.fileno.return_value = 17
        context = MagicMock()
        context.__enter__.return_value = handle

        with patch.object(Path, "open", return_value=context) as open_file:
            with patch(
                "scripts.build_target_acceptance_manifest.os.fsync"
            ) as fsync:
                _fsync_copied_file(Path("evidence.summary.json"))

        open_file.assert_called_once_with("rb+")
        handle.flush.assert_called_once_with()
        fsync.assert_called_once_with(17)

    def test_manifest_roundtrip_and_tamper_detection(self) -> None:
''',
    label="Windows fsync regression test",
)
test_path.write_text(test, encoding="utf-8")

print("Patched manifest evidence fsync for writable Windows descriptors and added regression coverage.")

# tests/output/test_staging_output_uploader.py

from __future__ import annotations

from pathlib import Path

import pytest

from core.output.staging_output_uploader import StagingFolderOutputUploader


def test_staging_output_uploader_copies_outputs_and_generates_download_urls(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    txt_file = source_dir / "transcript.txt"
    json_file = source_dir / "transcript.json"
    txt_file.write_text("hello", encoding="utf-8")
    json_file.write_text('{"ok":true}', encoding="utf-8")

    staging_dir = tmp_path / "staging"

    uploader = StagingFolderOutputUploader(
        staging_dir=staging_dir,
        download_base_url="https://downloads.example.com/files",
    )

    result = uploader.upload(
        job_id="job-123",
        outputs={
            "txt": str(txt_file),
            "json": str(json_file),
        },
    )

    staged_txt = staging_dir / "job-123" / "transcript.txt"
    staged_json = staging_dir / "job-123" / "transcript.json"

    assert staged_txt.exists()
    assert staged_json.exists()
    assert staged_txt.read_text(encoding="utf-8") == "hello"
    assert staged_json.read_text(encoding="utf-8") == '{"ok":true}'

    assert result.job_id == "job-123"
    assert result.outputs["txt"].destination == str(staged_txt)
    assert result.outputs["json"].destination == str(staged_json)
    assert result.outputs["txt"].download_url == (
        "https://downloads.example.com/files/job-123/transcript.txt"
    )
    assert result.outputs["json"].download_url == (
        "https://downloads.example.com/files/job-123/transcript.json"
    )


def test_staging_output_uploader_raises_when_output_file_missing(tmp_path):
    uploader = StagingFolderOutputUploader(
        staging_dir=tmp_path / "staging",
        download_base_url="https://downloads.example.com/files",
    )

    with pytest.raises(FileNotFoundError, match="Generated output does not exist"):
        uploader.upload(
            job_id="job-123",
            outputs={"txt": str(tmp_path / "missing.txt")},
        )


def test_staging_output_uploader_is_idempotent_for_existing_job_folder(tmp_path):
    source_file = tmp_path / "transcript.txt"
    source_file.write_text("hello", encoding="utf-8")

    staging_dir = tmp_path / "staging"
    existing_job_dir = staging_dir / "job-123"
    existing_job_dir.mkdir(parents=True)

    uploader = StagingFolderOutputUploader(
        staging_dir=staging_dir,
        download_base_url="https://downloads.example.com/files",
    )

    result = uploader.upload(
        job_id="job-123",
        outputs={"txt": str(source_file)},
    )

    staged_file = existing_job_dir / "transcript.txt"
    assert staged_file.exists()
    assert result.outputs["txt"].destination == str(staged_file)
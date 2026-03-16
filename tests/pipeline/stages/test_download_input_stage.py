from __future__ import annotations

from pathlib import Path

import pytest

from core.job.job import Job
from core.pipeline.context import JobContext
from core.pipeline.stage import StageStatus
from core.pipeline.stages.download_input_stage import DownloadInputStage


def test_download_input_stage_copies_local_file_into_workspace(tmp_path: Path) -> None:
    source_file = tmp_path / "source.mp4"
    source_file.write_bytes(b"video-data")

    job = Job(job_id="job-123", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-123",
        request={"job_id": "job-123", "input_uri": str(source_file)},
        workspace_dir=Path(job.root),
        input_uri=str(source_file),
        metadata={"job": job},
    )

    stage = DownloadInputStage()

    result = stage.run(context)

    expected_path = Path(job.input_dir) / "source.mp4"

    assert result.status == StageStatus.SUCCESS
    assert result.context is context
    assert context.input_path == expected_path
    assert expected_path.exists()
    assert expected_path.read_bytes() == b"video-data"
    assert result.metadata == {
        "job_id": "job-123",
        "input_uri": str(source_file),
        "input_path": str(expected_path),
    }


def test_download_input_stage_copies_file_uri_into_workspace(tmp_path: Path) -> None:
    source_file = tmp_path / "clip.wav"
    source_file.write_bytes(b"audio-data")

    job = Job(job_id="job-456", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-456",
        request={"job_id": "job-456", "input_uri": source_file.as_uri()},
        workspace_dir=Path(job.root),
        input_uri=source_file.as_uri(),
        metadata={"job": job},
    )

    stage = DownloadInputStage()

    result = stage.run(context)

    expected_path = Path(job.input_dir) / "clip.wav"

    assert result.status == StageStatus.SUCCESS
    assert context.input_path == expected_path
    assert expected_path.read_bytes() == b"audio-data"


def test_download_input_stage_downloads_http_input_into_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = Job(job_id="job-789", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-789",
        request={"job_id": "job-789", "input_uri": "https://example.com/media/test.mp3"},
        workspace_dir=Path(job.root),
        input_uri="https://example.com/media/test.mp3",
        metadata={"job": job},
    )

    captured: dict[str, str] = {}

    def fake_urlretrieve(url: str, filename: str | Path):
        captured["url"] = url
        captured["filename"] = str(filename)
        Path(filename).write_bytes(b"downloaded-audio")
        return str(filename), None

    monkeypatch.setattr(
        "core.pipeline.stages.download_input_stage.urlretrieve",
        fake_urlretrieve,
    )

    stage = DownloadInputStage()

    result = stage.run(context)

    expected_path = Path(job.input_dir) / "test.mp3"

    assert result.status == StageStatus.SUCCESS
    assert captured["url"] == "https://example.com/media/test.mp3"
    assert captured["filename"] == str(expected_path)
    assert context.input_path == expected_path
    assert expected_path.read_bytes() == b"downloaded-audio"


def test_download_input_stage_raises_when_input_uri_missing(tmp_path: Path) -> None:
    job = Job(job_id="job-missing", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-missing",
        request={"job_id": "job-missing"},
        workspace_dir=Path(job.root),
        input_uri=None,
        metadata={"job": job},
    )

    stage = DownloadInputStage()

    with pytest.raises(ValueError, match="context.input_uri is required"):
        stage.run(context)


def test_download_input_stage_raises_when_workspace_is_missing_and_no_job_metadata(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "source.mp4"
    source_file.write_bytes(b"video-data")

    context = JobContext(
        job_id="job-no-workspace",
        request={"job_id": "job-no-workspace", "input_uri": str(source_file)},
        workspace_dir=None,
        input_uri=str(source_file),
    )

    stage = DownloadInputStage()

    with pytest.raises(ValueError, match="context.workspace_dir is required"):
        stage.run(context)


def test_download_input_stage_raises_for_missing_local_source(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.mp4"
    job = Job(job_id="job-missing-file", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-missing-file",
        request={"job_id": "job-missing-file", "input_uri": str(missing_file)},
        workspace_dir=Path(job.root),
        input_uri=str(missing_file),
        metadata={"job": job},
    )

    stage = DownloadInputStage()

    with pytest.raises(FileNotFoundError, match="Input source does not exist"):
        stage.run(context)


def test_download_input_stage_raises_for_unsupported_uri_scheme(tmp_path: Path) -> None:
    job = Job(job_id="job-unsupported", base_dir=str(tmp_path / "jobs"))
    context = JobContext(
        job_id="job-unsupported",
        request={"job_id": "job-unsupported", "input_uri": "s3://bucket/file.mp4"},
        workspace_dir=Path(job.root),
        input_uri="s3://bucket/file.mp4",
        metadata={"job": job},
    )

    stage = DownloadInputStage()

    with pytest.raises(ValueError, match="Unsupported input URI scheme: s3"):
        stage.run(context)
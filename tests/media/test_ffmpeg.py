import shutil
import tempfile
import unittest
from pathlib import Path

from core.media.ffmpeg_utils import FFmpeg
from core.utils import CommandError


class TestFFmpeg(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.ffmpeg = FFmpeg()

        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # ---------- helpers ----------

    def create_test_wav(self, output_path: Path, duration_seconds: int = 1):
        command = [
            self.ffmpeg.ffmpeg_bin,
            "-y",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=16000:cl=mono",
            "-t",
            str(duration_seconds),
            str(output_path),
        ]
        self.ffmpeg.run_command(command)

    # ---------- init / binary validation ----------

    def test_init_creates_ffmpeg_instance(self):
        self.assertIsInstance(self.ffmpeg, FFmpeg)

    def test_init_raises_for_missing_ffmpeg_binary(self):
        with self.assertRaises(CommandError):
            FFmpeg(ffmpeg_bin="missing-ffmpeg-binary-xyz", ffprobe_bin="ffprobe")

    def test_init_raises_for_missing_ffprobe_binary(self):
        with self.assertRaises(CommandError):
            FFmpeg(ffmpeg_bin="ffmpeg", ffprobe_bin="missing-ffprobe-binary-xyz")

    # ---------- probe_media ----------

    def test_probe_media_returns_metadata_for_valid_wav(self):
        input_file = self.input_dir / "test.wav"
        self.create_test_wav(input_file)

        metadata = self.ffmpeg.probe_media(input_file)

        self.assertIsInstance(metadata, dict)
        self.assertIn("format", metadata)
        self.assertIn("streams", metadata)

    def test_probe_media_raises_for_missing_file(self):
        missing_file = self.input_dir / "missing.wav"

        with self.assertRaises(CommandError):
            self.ffmpeg.probe_media(missing_file)

    def test_probe_media_contains_audio_stream_metadata(self):
        input_file = self.input_dir / "test.wav"
        self.create_test_wav(input_file)

        metadata = self.ffmpeg.probe_media(input_file)

        self.assertIn("streams", metadata)
        self.assertGreater(len(metadata["streams"]), 0)

        audio_streams = [s for s in metadata["streams"] if s.get("codec_type") == "audio"]
        self.assertGreater(len(audio_streams), 0)

        stream = audio_streams[0]
        self.assertEqual(stream.get("codec_type"), "audio")
        self.assertIn("sample_rate", stream)
        self.assertIn("channels", stream)

    # ---------- extract_audio ----------

    def test_extract_audio_creates_output_file(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "extracted.wav"

        self.create_test_wav(input_file)

        result = self.ffmpeg.extract_audio(input_file, output_file)

        self.assertEqual(result, output_file)
        self.assertTrue(output_file.exists())
        self.assertTrue(output_file.is_file())

    def test_extract_audio_raises_for_missing_input(self):
        missing_file = self.input_dir / "missing.wav"
        output_file = self.output_dir / "extracted.wav"

        with self.assertRaises(CommandError):
            self.ffmpeg.extract_audio(missing_file, output_file)

    def test_extract_audio_creates_parent_directory_if_missing(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "nested" / "deeper" / "extracted.wav"

        self.create_test_wav(input_file)

        result = self.ffmpeg.extract_audio(input_file, output_file)

        self.assertEqual(result, output_file)
        self.assertTrue(output_file.parent.exists())
        self.assertTrue(output_file.exists())

    # ---------- convert_to_wav ----------

    def test_convert_to_wav_creates_output_file(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "clean.wav"

        self.create_test_wav(input_file)

        result = self.ffmpeg.convert_to_wav(input_file, output_file)

        self.assertEqual(result, output_file)
        self.assertTrue(output_file.exists())
        self.assertTrue(output_file.is_file())

    def test_convert_to_wav_raises_for_missing_input(self):
        missing_file = self.input_dir / "missing.wav"
        output_file = self.output_dir / "clean.wav"

        with self.assertRaises(CommandError):
            self.ffmpeg.convert_to_wav(missing_file, output_file)

    def test_convert_to_wav_outputs_expected_sample_rate(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "clean.wav"

        self.create_test_wav(input_file)

        self.ffmpeg.convert_to_wav(input_file, output_file, sample_rate=16000, channels=1)
        metadata = self.ffmpeg.probe_media(output_file)

        audio_streams = [s for s in metadata["streams"] if s.get("codec_type") == "audio"]
        self.assertGreater(len(audio_streams), 0)

        sample_rate = int(audio_streams[0]["sample_rate"])
        self.assertEqual(sample_rate, 16000)

    def test_convert_to_wav_outputs_expected_channel_count(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "clean.wav"

        self.create_test_wav(input_file)

        self.ffmpeg.convert_to_wav(input_file, output_file, sample_rate=16000, channels=1)
        metadata = self.ffmpeg.probe_media(output_file)

        audio_streams = [s for s in metadata["streams"] if s.get("codec_type") == "audio"]
        self.assertGreater(len(audio_streams), 0)

        channels = int(audio_streams[0]["channels"])
        self.assertEqual(channels, 1)

    def test_convert_to_wav_creates_parent_directory_if_missing(self):
        input_file = self.input_dir / "source.wav"
        output_file = self.output_dir / "nested" / "deeper" / "clean.wav"

        self.create_test_wav(input_file)

        result = self.ffmpeg.convert_to_wav(input_file, output_file)

        self.assertEqual(result, output_file)
        self.assertTrue(output_file.parent.exists())
        self.assertTrue(output_file.exists())

    # ---------- get_audio_duration ----------

    def test_get_audio_duration_returns_float(self):
        input_file = self.input_dir / "test.wav"
        self.create_test_wav(input_file, duration_seconds=2)

        duration = self.ffmpeg.get_audio_duration(input_file)

        self.assertIsInstance(duration, float)
        self.assertGreater(duration, 0.0)

    def test_get_audio_duration_raises_for_missing_file(self):
        missing_file = self.input_dir / "missing.wav"

        with self.assertRaises(CommandError):
            self.ffmpeg.get_audio_duration(missing_file)


if __name__ == "__main__":
    unittest.main()
import shutil
import tempfile
import unittest
from pathlib import Path

from core.job import Job


class TestJob(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_base_jobs_directory_when_missing(self):
        base_dir = Path(self.temp_dir) / "jobs"

        self.assertFalse(base_dir.exists())

        job = Job.create(base_dir=str(base_dir))

        self.assertTrue(base_dir.exists())
        self.assertTrue(base_dir.is_dir())
        self.assertTrue((base_dir / job.job_id).exists())

    def test_creates_full_workspace_directory_structure(self):
        job = Job.create(base_dir=self.temp_dir)

        self.assertTrue(job.root.exists())
        self.assertTrue(job.input_dir.exists())
        self.assertTrue(job.audio_dir.exists())
        self.assertTrue(job.output_dir.exists())
        self.assertTrue(job.logs_dir.exists())

        self.assertTrue(job.root.is_dir())
        self.assertTrue(job.input_dir.is_dir())
        self.assertTrue(job.audio_dir.is_dir())
        self.assertTrue(job.output_dir.is_dir())
        self.assertTrue(job.logs_dir.is_dir())

    def test_generates_unique_job_id(self):
        job1 = Job.create(base_dir=self.temp_dir)
        job2 = Job.create(base_dir=self.temp_dir)

        self.assertNotEqual(job1.job_id, job2.job_id)

    def test_job_id_is_non_empty_string(self):
        job = Job.create(base_dir=self.temp_dir)

        self.assertIsInstance(job.job_id, str)
        self.assertTrue(job.job_id)
        self.assertGreater(len(job.job_id), 0)

    def test_paths_are_built_under_base_dir(self):
        job = Job.create(base_dir=self.temp_dir)

        expected_root = Path(self.temp_dir) / job.job_id
        expected_input = expected_root / "input"
        expected_audio = expected_root / "audio"
        expected_output = expected_root / "output"
        expected_logs = expected_root / "logs"

        self.assertEqual(job.root, expected_root)
        self.assertEqual(job.input_dir, expected_input)
        self.assertEqual(job.audio_dir, expected_audio)
        self.assertEqual(job.output_dir, expected_output)
        self.assertEqual(job.logs_dir, expected_logs)

    def test_paths_method_returns_expected_dictionary(self):
        job = Job.create(base_dir=self.temp_dir)

        result = job.paths()

        self.assertEqual(
            result,
            {
                "job_id": job.job_id,
                "root": str(job.root),
                "input": str(job.input_dir),
                "audio": str(job.audio_dir),
                "output": str(job.output_dir),
                "logs": str(job.logs_dir),
            },
        )

    def test_can_use_nested_base_directory(self):
        nested_base_dir = Path(self.temp_dir) / "var" / "data" / "jobs"

        job = Job.create(base_dir=str(nested_base_dir))

        self.assertTrue(nested_base_dir.exists())
        self.assertEqual(job.base_dir, nested_base_dir)
        self.assertTrue(job.root.exists())

    def test_can_use_path_object_as_base_dir(self):
        base_dir = Path(self.temp_dir) / "jobs"

        job = Job.create(base_dir=base_dir)

        self.assertEqual(job.base_dir, base_dir)
        self.assertTrue(job.root.exists())

    def test_existing_base_directory_does_not_fail(self):
        base_dir = Path(self.temp_dir) / "jobs"
        base_dir.mkdir(parents=True, exist_ok=True)

        job = Job.create(base_dir=str(base_dir))

        self.assertTrue(base_dir.exists())
        self.assertTrue(job.root.exists())

    def test_multiple_jobs_can_be_created_under_same_base_directory(self):
        base_dir = Path(self.temp_dir) / "jobs"

        job1 = Job.create(base_dir=str(base_dir))
        job2 = Job.create(base_dir=str(base_dir))
        job3 = Job.create(base_dir=str(base_dir))

        self.assertTrue((base_dir / job1.job_id).exists())
        self.assertTrue((base_dir / job2.job_id).exists())
        self.assertTrue((base_dir / job3.job_id).exists())

        self.assertEqual(len({job1.job_id, job2.job_id, job3.job_id}), 3)

    def test_workspace_directories_are_empty_on_creation(self):
        job = Job.create(base_dir=self.temp_dir)

        self.assertEqual(list(job.input_dir.iterdir()), [])
        self.assertEqual(list(job.audio_dir.iterdir()), [])
        self.assertEqual(list(job.output_dir.iterdir()), [])
        self.assertEqual(list(job.logs_dir.iterdir()), [])

    def test_workspace_creation_is_idempotent_for_existing_directories(self):
        job = Job.create(base_dir=self.temp_dir)

        # Call internal method again to confirm no exception and no damage
        job._create_workspace()

        self.assertTrue(job.root.exists())
        self.assertTrue(job.input_dir.exists())
        self.assertTrue(job.audio_dir.exists())
        self.assertTrue(job.output_dir.exists())
        self.assertTrue(job.logs_dir.exists())

    def test_string_paths_returned_by_paths_method_are_absolute_or_relative_consistent(self):
        job = Job.create(base_dir=self.temp_dir)
        result = job.paths()

        self.assertEqual(Path(result["root"]), job.root)
        self.assertEqual(Path(result["input"]), job.input_dir)
        self.assertEqual(Path(result["audio"]), job.audio_dir)
        self.assertEqual(Path(result["output"]), job.output_dir)
        self.assertEqual(Path(result["logs"]), job.logs_dir)

    def test_job_attributes_are_path_objects(self):
        job = Job.create(base_dir=self.temp_dir)

        self.assertIsInstance(job.base_dir, Path)
        self.assertIsInstance(job.root, Path)
        self.assertIsInstance(job.input_dir, Path)
        self.assertIsInstance(job.audio_dir, Path)
        self.assertIsInstance(job.output_dir, Path)
        self.assertIsInstance(job.logs_dir, Path)


if __name__ == "__main__":
    unittest.main()

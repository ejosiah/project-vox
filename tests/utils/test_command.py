import unittest

from core.utils import CommandError, CommandResult, CommandRunnerMixin


class DummyCommandRunner(CommandRunnerMixin):
    pass


class TestCommandRunnerMixin(unittest.TestCase):
    def setUp(self):
        self.runner = DummyCommandRunner()

    def test_run_command_failure_propagates_as_command_error(self):
        with self.assertRaises(CommandError):
            self.runner.run_command(["this-command-does-not-exist-xyz"])

    def test_run_command_success_returns_command_result(self):
        result = self.runner.run_command(["python3", "--version"])

        self.assertIsInstance(result, CommandResult)
        self.assertEqual(result.returncode, 0)
        self.assertIsInstance(result.stdout, str)
        self.assertIsInstance(result.stderr, str)
        self.assertEqual(result.command[0], "python3")


if __name__ == "__main__":
    unittest.main()
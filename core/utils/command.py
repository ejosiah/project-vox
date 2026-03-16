import subprocess
from dataclasses import dataclass


class CommandError(Exception):
    pass


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


class CommandRunnerMixin:

    def run_command(self, command: list[str]) -> CommandResult:
        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise CommandError(f"Command not found: {command[0]}") from exc

        result = CommandResult(
            command=command,
            returncode=process.returncode,
            stdout=process.stdout,
            stderr=process.stderr,
        )

        if process.returncode != 0:
            raise CommandError(
                f"Command failed ({process.returncode})\n"
                f"{' '.join(command)}\n\n"
                f"{process.stderr.strip()}"
            )

        return result
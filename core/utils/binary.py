import shutil
from .command import CommandError


class BinaryValidationMixin:

    def validate_binary(self, binary: str) -> None:
        if shutil.which(binary) is None:
            raise CommandError(f"Required binary not found: {binary}")

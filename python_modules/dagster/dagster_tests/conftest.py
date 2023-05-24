import os
import subprocess
import sys

import dagster._seven as seven
import pytest
from dagster._core.definitions.decorators.op_decorator import CODE_ORIGIN_ENABLED

IS_BUILDKITE = os.getenv("BUILDKITE") is not None

# Suggested workaround in https://bugs.python.org/issue37380 for subprocesses
# failing to open sporadically on windows after other subprocesses were closed.
# Fixed in later versions of Python but never back-ported, see the bug for details.
if seven.IS_WINDOWS and sys.version_info[0] == 3 and sys.version_info[1] == 6:
    subprocess._cleanup = lambda: None  # type: ignore # noqa: SLF001


@pytest.fixture
def ignore_code_origin():
    CODE_ORIGIN_ENABLED[0] = False

    yield

    CODE_ORIGIN_ENABLED[0] = True

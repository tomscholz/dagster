import pytest
from dagster._core.definitions.decorators.op_decorator import CODE_ORIGIN_ENABLED


@pytest.fixture
def ignore_code_origin():
    CODE_ORIGIN_ENABLED[0] = False

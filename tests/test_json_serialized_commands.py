import pytest
from serialized_redis import JSONSerializedRedis
from tests import common_test_commands
from .conftest import _get_client


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(JSONSerializedRedis, request, **kwargs)


class TestJSONSerializedRedis(common_test_commands.TestRedisCommands):
    pass

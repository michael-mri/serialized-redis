import pytest

from serialized_redis import JSONSerializedRedis
from tests import common_commands_tests, common_pubsub_tests, common_pipeline_tests

from .conftest import _get_client


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(JSONSerializedRedis, request, **kwargs)


class TestRedisCommands(common_commands_tests.TestRedisCommands):
    pass


class TestPubSubMessages(common_pubsub_tests.TestPubSubMessages):
    pass


class TestPubSubPubSubSubcommands(common_pubsub_tests.TestPubSubPubSubSubcommands):
        pass


class TestPubSubRedisDown(common_pubsub_tests.TestPubSubRedisDown):
    pass


class TestPubSubSubscribeUnsubscribe(common_pubsub_tests.TestPubSubSubscribeUnsubscribe):
    pass


class TestPipeline(common_pipeline_tests.TestPipeline):
    pass

import pytest
from serialized_redis import MsgpackSerializedRedis
from tests import common_commands_tests, common_pubsub_tests, common_pipeline_tests
from .conftest import _get_client


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(MsgpackSerializedRedis, request, **kwargs)


class TestRedisCommands(common_commands_tests.TestRedisCommands):

    def test_incr(self, r):
        with pytest.raises(NotImplementedError):
            r.incr('a')

    def test_incrby(self, r):
        with pytest.raises(NotImplementedError):
            r.incrby('a')

    def test_incrbyfloat(self, r):
        with pytest.raises(NotImplementedError):
            r.incrbyfloat('a')

    def test_sort_by(self, r):
        r['score:1'] = 8
        r['score:2'] = 3
        r['score:3'] = 5
        # Only supported for interger values
        r.rpush('a', 3, 2, 1)
        with pytest.raises(NotImplementedError):
            r.sort('a', by='score:*')

    def test_sort_alpha(self, r):
        with pytest.raises(NotImplementedError):
            r.sort('a', alpha=True)

    def test_sort_all_options(self, r):
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


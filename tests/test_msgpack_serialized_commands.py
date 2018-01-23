import pytest
from serialized_redis import MsgpackSerializedRedis
from tests import common_test_commands
from .conftest import _get_client


@pytest.fixture()
def r(request, **kwargs):
    return _get_client(MsgpackSerializedRedis, request, **kwargs)


class TestMsgpackSerializedRedis(common_test_commands.TestRedisCommands):

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
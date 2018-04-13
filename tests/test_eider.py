# Copyright 2017 Semiconductor Components Industries LLC (d/b/a "ON
# Semiconductor")
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    test_eider
    ~~~~~~~~~~

    Unit tests for eider.
"""

from asyncio import (
    CancelledError, coroutine, get_event_loop, Future, new_event_loop, sleep)
from functools import reduce
from gc import collect
from inspect import signature
from numbers import Number
from operator import mul
from sys import version_info
from threading import Thread

import pytest

import eider


PORT = 12345
URL = 'ws://localhost:{}/'.format(PORT)


if version_info >= (3, 6):
    # Python 3.6 has async comprehensions
    exec("""async def aiter2list(it):
                them = await it
                return [x async for x in them]
         """)

elif version_info >= (3, 5):
    # Python 3.5 has 'async for'
    exec("""async def aiter2list(it):
                them = await it
                xs = []
                async for x in them:
                    xs.append(x)
                return xs
         """)

else:
    # Python 3.4 doesn't have built-in support for async iterators
    @coroutine
    def aiter2list(it):
        them = yield from it
        xs = []
        yield from eider.async_for(them, coroutine(xs.append))
        return xs


class Value(eider.LocalObject):
    """Represents a numeric value."""

    def __init__(self, lsession, x):
        super().__init__(lsession)
        self._x = x

    def val(self):
        return self._x

    @coroutine
    def set_val(self, x):
        self._x = (yield from get_value(x))

    @coroutine
    def add(self, x):
        """Add another value to the value."""
        self._x += (yield from get_value(x))

    @coroutine
    def subtract(self, x):
        self._x -= (yield from get_value(x))

    @coroutine
    def multiply(self, x):
        self._x *= (yield from get_value(x))

    @coroutine
    def divide(self, x):
        self._x /= (yield from get_value(x))


@coroutine
def get_value(x):
    # x may be a number, a local Value, or a remote Value
    if isinstance(x, Number):
        return x  # number
    else:
        x = x.val()
        if isinstance(x, Number):
            return x  # local Value
        else:
            return (yield from x)  # remote Value


class Range(eider.LocalObject):

    def __init__(self, lsession, start, stop):
        super().__init__(lsession)
        self._start = start
        self._stop = stop

    def iter(self):
        return self

    def next(self):
        i = self._start
        if i >= self._stop:
            return {'done': True}
        self._start = i + 1
        return {'value': i}


class Sequence(eider.LocalObject):

    def __init__(self, lsession, seq):
        super().__init__(lsession)
        self._seq = seq

    def get(self, i):
        return self._seq[i]


class API(eider.LocalRoot):

    _newables = [Value, Range, Sequence]

    def num_objects(self):
        return len(self._lsession.objects)

    def call(self, f, *args):
        return f(*args)

    def store_cb(self, cb):
        self.cb = cb

    def call_cb(self, *args):
        return self.cb(*args)

    def passthru(self, x):
        return x

    def native(self, x):
        return NativeObject(x)


class LocalAPI(API):

    def product(self, *args):
        return reduce(mul, args)

    def square(self, x):
        return x * x


class RemoteAPI(API):

    target = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if RemoteAPI.target is None:
            RemoteAPI.target = Future(loop=self._lsession.conn.loop)
        self._cancelled = Future(loop=self._lsession.conn.loop)

    def sum(self, *args):
        return sum(args)

    def cancellable(self):
        def on_done(fut):
            self._cancelled.set_result(fut.cancelled())
        fut = Future(loop=self._lsession.conn.loop)
        fut.add_done_callback(on_done)
        return fut

    @coroutine
    def cancelled(self):
        return (yield from self._cancelled)

    @coroutine
    def map(self, f: 'Callable', xs: list, async_=True) -> list:
        return [(yield from f(x)) for x in xs] if async_ else list(map(f, xs))

    def getattr(self, obj, attr):
        with obj as o:
            return getattr(o, attr)

    def set_target(self):
        RemoteAPI.target.set_result(self._lsession.conn)

    @coroutine
    def bridge(self):
        return eider.Bridge(self._lsession, (yield from RemoteAPI.target))


class TargetAPI(API):

    def join(self, s, xs):
        return s.join(xs)


class NativeObject:

    def __init__(self, x):
        self.x = x

    def add(self, x):
        self.x += x

    def get(self):
        return self.x


def native_function(s):
    return s + ' native'


@pytest.fixture(scope='module')
def server():
    t = Thread(target=eider.serve,
               args=[PORT, new_event_loop()],
               kwargs={'root': RemoteAPI, 'handle_signals': False},
               daemon=True)
    t.start()


@pytest.yield_fixture(scope='module')
def conn(server):
    with eider.BlockingConnection(URL, root=LocalAPI) as conn:
        yield conn


@pytest.yield_fixture(scope='module')
def conn_async(server):
    conn = eider.Connection(URL, root=LocalAPI)
    try:
        yield conn
    finally:
        conn.close()
        get_event_loop().run_until_complete(conn.wait_closed())


@pytest.yield_fixture
def lroot(conn):
    with conn.create_local_session() as lroot:
        yield lroot


@pytest.yield_fixture
def rroot(conn):
    with conn.create_session() as rroot:
        yield rroot


@pytest.yield_fixture
def rroot_async(conn_async):
    with conn_async.create_session() as rroot:
        yield rroot


@pytest.yield_fixture
def rroot_codec(conn):
    with conn.create_session('json', 'json') as rroot:
        yield rroot


@pytest.yield_fixture
def rroot_msgpack(conn):
    with conn.create_session('msgpack', 'msgpack') as rroot:
        yield rroot


@pytest.yield_fixture(scope='module')
def conn_msgpack(server):
    with eider.BlockingConnection(URL, lformat='msgpack') as conn:
        yield conn


@pytest.yield_fixture
def rroot_bin(conn_msgpack):
    with conn_msgpack.create_session() as rroot:
        yield rroot


@pytest.fixture(scope='module')
def target(server):
    def run():
        @coroutine
        def receive():
            conn = eider.Connection(URL, loop, root=TargetAPI)
            with conn.create_session() as rroot:
                yield from rroot.set_target()
            yield from conn.wait_closed()

        loop = new_event_loop()
        loop.run_until_complete(receive())

    Thread(target=run, daemon=True).start()


@pytest.yield_fixture
def broot(rroot, target):
    with rroot.bridge() as broot:
        yield broot


def test_call(rroot):
    """Call a remote method."""
    assert 17 == rroot.sum(3, 5, 9)


def test_call_async(rroot_async):
    """Call a remote method asynchronously."""
    @coroutine
    def test():
        return (yield from rroot_async.sum(33, 55, 99))
    assert 187 == get_event_loop().run_until_complete(test())


def test_cancel(rroot_async):
    """Cancel a remote method call."""
    loop = get_event_loop()
    fut = rroot_async.cancellable()
    loop.call_soon(fut.cancel)
    try:
        loop.run_until_complete(fut)
    except CancelledError:
        pass
    else:
        assert False
    assert loop.run_until_complete(rroot_async.cancelled())


def test_call_codec(rroot_codec):
    """Call using separately-encoded message bodies."""
    assert 42 == rroot_codec.sum(24, 10, 8)


def test_new(rroot):
    """Create a remote object."""
    assert 2 == rroot.new_Value(2).val()


def test_prop(rroot):
    """Set a remote property."""
    rval = rroot.new_Value(2)
    rval.val = 4
    assert 4 == rval.val()


def test_prop_auto(rroot):
    """Create a new remote property."""
    rval = rroot.new_Value(3)
    rval.extra = 5
    assert 5 == rval.extra()


def test_prop_auto_forbidden(rroot):
    """Assign to a forbidden remote property."""
    rval = rroot.new_Value(4)
    try:
        rval.release = 6
    except AttributeError:
        return
    assert False


def test_error_notfound(rroot):
    """Call a nonexistent remote method."""
    try:
        rroot.foo(42)
    except AttributeError:
        return
    assert False


def test_error_runtime(rroot):
    """Call a remote method that raises an exception."""
    try:
        rroot.new_Value(42).divide(0)
    except ZeroDivisionError as exc:
        assert isinstance(exc.__cause__, eider.RemoteError)
        return
    assert False


def test_refcount(rroot):
    """Release a remote object."""
    n = rroot.num_objects()
    with rroot.new_Value(0):
        assert n + 1 == rroot.num_objects()
    assert n == rroot.num_objects()


def test_gc(rroot):
    """Garbage-collect a remote object."""
    n = rroot.num_objects()
    rval = rroot.new_Value(0)
    assert n + 1 == rroot.num_objects()
    del rval

    # make sure RemoteObject._close() (triggered by RemoteObject.__del__)
    # completes
    collect()
    get_event_loop().run_until_complete(sleep(0.1))

    assert n == rroot.num_objects()


def test_with(rroot):
    """Try to access a remote object after it has been released."""
    with rroot.new_Value(42) as rval:
        rval.add(1)
    try:
        rval.val()
    except LookupError:
        return
    assert False


def test_session(conn):
    """Try to access a remote object after its session has been closed."""
    with conn.create_session() as rroot:
        rval = rroot.new_Value(0)
    try:
        rval.val()
    except LookupError:
        return
    assert False


def test_iter(rroot):
    """Iterate over a remote object."""
    assert [3, 4, 5, 6] == [x for x in rroot.new_Range(3, 7)]


def test_iter_async(rroot_async):
    """Iterate over a remote object asynchronously."""
    assert [38, 39, 40, 41] == get_event_loop().run_until_complete(
        aiter2list(rroot_async.new_Range(38, 42)))


def test_iter_seq(rroot):
    """Iterate over a remote sequence."""
    seq = ['foo', 'bar', 42, 'spam']
    assert seq == [x for x in rroot.new_Sequence(seq)]


def test_iter_seq_async(rroot_async):
    """Iterate over a remote sequence asynchronously."""
    seq = ['foo', 'baz', 99, 'eggs']
    assert seq == get_event_loop().run_until_complete(
        aiter2list(rroot_async.new_Sequence(seq)))


def test_help_object(rroot):
    """Get documentation for a remote object."""
    assert "Represents a numeric value." == rroot.new_Value(42).help()


def test_help_method(rroot):
    """Get documentation for a remote method."""
    assert "Add another value to the value." == rroot.new_Value(42).add.help()


def test_dir(rroot):
    """List remote object's methods."""
    assert """add addref dir divide help multiply release set_val signature
        subtract taxa val""".split() == rroot.new_Value(42).dir()


def test_taxa(rroot):
    """List remote object's base classes."""
    assert ['RemoteAPI', 'API'] == rroot.taxa()


def test_signature(rroot):
    """Get type signature for a remote method."""
    sig = rroot.map.signature()
    assert {
        'defaults': {'async_': True},
        'params': [['f', 'Callable'], ['xs', 'list'], ['async_', None]],
        'return': 'list'
    } == sig

    # test unmarshal_signature as well
    def g(f: 'Callable', xs: list, async_=True) -> list:
        pass
    assert signature(g) == eider.unmarshal_signature(sig)


def test_callback_async(lroot, rroot):
    """Call a local method remotely, without remote post-processing."""
    assert 135 == rroot.call(lroot.product, 3, 5, 9)


def test_callback_sync(lroot, rroot):
    """Call a local method remotely, with remote post-processing."""
    assert [1, 4, 9, 16] == rroot.map(lroot.square, [1, 2, 3, 4])


def test_callback_error_async(lroot, rroot):
    """Call an exception-raising local method remotely, without remote
    post-processing."""
    try:
        rroot.call(lroot.new_Value(42).divide, 0)
    except ZeroDivisionError:
        return
    assert False


def test_callback_error_sync(lroot, rroot):
    """Call an exception-raising local method remotely, with remote
    post-processing."""
    lval = lroot.new_Value(42)
    try:
        rroot.map(lval.divide, [3, 1, 0, 7])
    except ZeroDivisionError:
        return
    assert False


def test_callfront(rroot):
    """Call a remote method remotely."""
    assert 66 == rroot.call(rroot.sum, 42, 24)


def test_rmarshal(rroot):
    """Return a remote method from a remote call."""
    assert 42 == rroot.getattr(rroot, 'sum')(19, 10, 13)


def test_lmarshal(lroot, rroot):
    """Return a local method from a remote call."""
    assert 120 == rroot.getattr(lroot, 'product')(8, 5, 3)


def test_lobject(lroot, rroot):
    """Pass a local object to a remote call."""
    with lroot.new_Value(3) as lval, rroot.new_Value(4) as rval:
        rval.add(lval)
        assert 7 == rval.val()


def test_native_lmarshal(rroot):
    """Pass a local native object to a remote call."""
    n = NativeObject(42)
    assert n is rroot.passthru(n)
    assert native_function is rroot.passthru(native_function)


def test_native_rmarshal(rroot):
    """Return a remote native object from a remote call."""
    rn = rroot.native(99)
    rn.add(1)
    assert 100 == rn.get()


def test_native_callback(rroot):
    """Call a native method remotely."""
    n = NativeObject(42)
    rroot.call(n.add, 3)
    assert 45 == n.x


def test_native_callback_function(rroot):
    """Call a native function remotely."""
    assert 'gone native' == rroot.call(native_function, 'gone')


def test_native_callback_lambda(rroot):
    """Call an anonymous native function remotely."""
    x = []
    rroot.store_cb(lambda y: x.append(y))
    rroot.call_cb(42)
    assert [42] == x


def test_bridge(broot):
    """Call a bridged method locally."""
    assert 'bridges are neat' == broot.join(' ',
                                            'bridges    are    neat'.split())


def test_bridge_session(rroot, target):
    """Try to access a bridged object after its bridge has been closed."""
    with rroot.bridge() as broot:
        bval = broot.new_Value(0)
    try:
        bval.val()
    except LookupError:
        return
    assert False


def test_bridge_error(broot):
    """Call a bridged method that raises an exception."""
    try:
        broot.new_Value(42).divide(0)
    except ZeroDivisionError:
        return
    assert False


def test_bridge_callback(lroot, broot):
    """Call a local method across a bridge."""
    assert 36 == broot.call(lroot.product, 3, 6, 2)


def test_bridge_callfront(broot):
    """Call a bridged method across a bridge."""
    assert 'a+b+c' == broot.call(broot.join, '+', 'abc')


def test_msgpack(rroot_msgpack):
    """Call using msgpack codec."""
    assert 11 == rroot_msgpack.sum(67, -59, 3)


def test_msgpack_binary(rroot_msgpack):
    """Pass binary data using msgpack."""
    buf = bytes(range(7))
    assert buf == rroot_msgpack.passthru(buf)


def test_msgpack_primary(rroot_bin):
    """Call with msgpack as the primary format."""
    assert 176 == rroot_bin.sum(3, 14, 159)


def test_marshal_outofband(rroot_msgpack):
    """Marshal object references out of band."""
    with rroot_msgpack.new_Value(6) as six:
        with rroot_msgpack.new_Value(11) as eleven:
            eleven.add(six)
            assert 17 == eleven.val()


def test_pass_oid(rroot_msgpack):
    """Pass dict with special "__*__" key (only works with out-of-band
    codecs)."""
    obj = {eider.OBJECT_ID: 42, "rsid": 99}
    assert obj == rroot_msgpack.passthru(obj)

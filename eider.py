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
    eider
    ~~~~~

    The Eider RPC protocol.
"""

from asyncio import (
    CancelledError, coroutine, Future, get_event_loop, iscoroutine, Queue,
    set_event_loop)
from base64 import b64encode
import builtins
from collections import defaultdict

try:
    from collections.abc import Coroutine
except ImportError:
    # Python 3.4
    Coroutine = object

from functools import partial
from inspect import getdoc, Parameter, signature, Signature
from io import StringIO
from json import dumps, loads as decode
from logging import DEBUG, getLogger
from sys import platform, version_info
from threading import local
from traceback import print_exception
from types import FunctionType, MethodType


try:
    import aiohttp
    from aiohttp import (
        __version__ as aiohttp_version, ClientSession, WSCloseCode, WSMsgType)
    from aiohttp.web import Application, run_app, WebSocketResponse
except ImportError:
    aiohttp = None

    from enum import IntEnum

    class WSCloseCode(IntEnum):
        OK = 1000
        GOING_AWAY = 1001
        UNSUPPORTED_DATA = 1003
        POLICY_VIOLATION = 1008
        INTERNAL_ERROR = 1011

    class WSMsgType(IntEnum):
        TEXT = 0x1
        BINARY = 0x2
        CLOSE = 0x8
        CLOSED = 0x101
        ERROR = 0x102


try:
    import websockets
    from websockets import (
        connect as ws_connect, ConnectionClosed, serve as ws_serve,
        WebSocketCommonProtocol)
except ImportError:
    websockets = None

    class ConnectionClosed(Exception):
        pass

    class WebSocketCommonProtocol:
        pass


WS_LIB_DEFAULT = 'aiohttp' if aiohttp else 'websockets' if websockets else None


__version__ = '1.0.0'


try:
    StopAsyncIteration = builtins.StopAsyncIteration  # added in Python 3.5
except AttributeError:
    class StopAsyncIteration(Exception):
        pass


@coroutine
def async_for(iterable, body):
    """Like 'async for', but properly cleans up remote resources.  If PEP 533
    is ever accepted, we can define RemoteIterator.__aiterclose__ and deprecate
    this function in favor of plain 'async for'."""
    iterator = type(iterable).__aiter__(iterable)
    anext = type(iterator).__anext__
    try:  # In Python 3.5+, this could be 'async with iterator:'.
        while True:
            try:
                x = yield from anext(iterator)
            except StopAsyncIteration:
                break
            else:
                stop = yield from body(x)
                if stop is not None:
                    return stop
    finally:
        yield from iterator.close()


class CoroutineContextManager(Coroutine):
    """This class allows a coroutine that returns an async context manager
    to be used directly in the 'async with' statement.  That is, instead of
    being forced to write

        y = await x()
        async with y as z:
            ...

    you can just write

        async with x() as z:
            ...

    This is a slight generalization of aiohttp's _RequestContextManager.
    """

    __slots__ = ('_coro', '_val')

    def __init__(self, coro):
        self._coro = coro

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    def __iter__(self):
        return self._coro.__iter__()

    def __await__(self):
        try:
            aw = self._coro.__await__
        except AttributeError:
            # Create a native coroutine object to work around PEP492's
            # restriction that the return value from __await__() must be an
            # iterator.
            ns = {'coro': self._coro}
            exec("""async def native_coro():
                        return await coro
                 """, ns)
            aw = ns['native_coro']().__await__
        return aw()

    @coroutine
    def __aenter__(self):
        self._val = val = yield from self._coro
        return (yield from val.__aenter__())

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        yield from self._val.__aexit__(exc_type, exc_value, traceback)


# Marker for marshallable object references within encoded data
OBJECT_ID = '__*__'


class Codec:

    registry = {}

    def __init__(self, name, encode, decode, inband=True):
        self.name = name
        self.encode = encode
        self.decode = decode
        self.inband = inband
        self.registry[name] = self

    @classmethod
    def byname(cls, name):
        if name is None:
            return None
        codec = cls.registry.get(name)
        if codec is None:
            raise LookupError('Unknown format: {}'.format(name))
        return codec


def marshal(conn, obj):
    try:
        _marshal = obj._marshal
    except AttributeError:
        if isinstance(obj, MethodType):
            method = obj.__name__
            obj = obj.__self__
            try:
                _marshal = obj._marshal
            except AttributeError:
                # native method
                pass
            else:
                # LocalObject method
                return dict(_marshal(), method=method)
        elif isinstance(obj, FunctionType):
            # native function or lambda
            method = 'call'
        else:
            # native object
            method = None
    else:
        # LocalObject
        return _marshal()

    return conn.lsessions[-1].marshal(obj, method)


def encode(conn, data):
    return dumps(data, default=partial(marshal, conn))


Codec('json', encode, decode)


try:
    from msgpack import ExtType, packb, unpackb
except ImportError:
    pass
else:
    def msgpack_default(conn, obj):
        return ExtType(0, packb(marshal(conn, obj), use_bin_type=True))

    def msgpack_encode(conn, data):
        return packb(data, use_bin_type=True,
                     default=partial(msgpack_default, conn))

    def msgpack_ext_hook(code, data):
        if code == 0:
            return Reference(unpackb(data, raw=False))
        return ExtType(code, data)

    def msgpack_decode(data):
        return unpackb(data, raw=False, ext_hook=msgpack_ext_hook)

    Codec('msgpack', msgpack_encode, msgpack_decode, False)


def marshal_annotation(annot):
    # cf. inspect.formatannotation
    if getattr(annot, '__module__', None) == 'typing':
        return repr(annot).replace('typing.', '')
    if isinstance(annot, type):
        # give the class's __repr__ a chance to give us something eval-able
        r = repr(annot)
        if r[0] == '<':
            # we want just 'Class', not '<class path.to.module.Class>'
            return annot.__name__
        return r
    return annot


def marshal_signature(sig):
    params = sig.parameters
    ret = sig.return_annotation
    return {
        'params': [
            (('*' if param.kind is Parameter.VAR_POSITIONAL else '') + name,
             (None if param.annotation is Parameter.empty else
              marshal_annotation(param.annotation)))
            for name, param in params.items()
            if param.kind in (Parameter.POSITIONAL_OR_KEYWORD,
                              Parameter.VAR_POSITIONAL)
        ],
        'defaults': {
            name: param.default
            for name, param in params.items()
            if param.default is not Parameter.empty
        },
        'return': None if ret is Signature.empty else marshal_annotation(ret)
    }


def unmarshal_annotation(annot):
    return getattr(builtins, annot, annot)


def unmarshal_signature(fields,
                        unmarshal_annotation=unmarshal_annotation):
    params = []
    defaults = fields['defaults']
    for name, annot in fields['params']:
        if name[0] == '*':
            name = name[1:]
            kind = Parameter.VAR_POSITIONAL
        else:
            kind = Parameter.POSITIONAL_OR_KEYWORD
        if annot is None:
            annot = Parameter.empty
        else:
            annot = unmarshal_annotation(annot)
        params.append(Parameter(name, kind,
                                default=defaults.get(name, Parameter.empty),
                                annotation=annot))
    ret = fields['return']
    return Signature(params,
                     return_annotation=(Signature.empty if ret is None else
                                        unmarshal_annotation(ret)))


class ProtocolError(Exception):
    pass


class DisconnectedError(Exception):
    pass


class RemoteError(Exception):
    pass


Error = Exception  # for JavaScript


class Reference:

    __slots__ = ('ref',)

    def __init__(self, ref):
        self.ref = ref

    def _marshal(self):
        return self.ref


class Session:

    __slots__ = ('conn', 'lcodec')

    def __init__(self, conn, lformat=None):
        self.conn = conn
        self.lcodec = Codec.byname(lformat)

    def unmarshal_all(self, rcodec, obj, srcid=None):
        if rcodec.inband:
            return self.unmarshal_all_inband(obj, srcid)
        return self.unmarshal_all_outofband(obj, srcid)

    def unmarshal_all_inband(self, obj, srcid=None):
        if isinstance(obj, dict):
            if OBJECT_ID in obj:
                return self.unmarshal(obj, srcid)
            return {k: self.unmarshal_all_inband(o, srcid)
                    for k, o in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self.unmarshal_all_inband(o, srcid) for o in obj]
        return obj

    def unmarshal_all_outofband(self, obj, srcid=None):
        if isinstance(obj, dict):
            return {k: self.unmarshal_all_outofband(o, srcid)
                    for k, o in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self.unmarshal_all_outofband(o, srcid) for o in obj]
        if isinstance(obj, Reference):
            return self.unmarshal(obj.ref, srcid)
        return obj

    def unmarshal(self, ref, srcid):
        obj = self.unmarshal_obj(ref, srcid)

        method = ref.get('method')
        if not method:
            return obj
        if method[:1] == '_':
            raise AttributeError(
                "Cannot access private attribute '{}' of '{}' object".format(
                    method, type(obj).__name__))

        return getattr(obj, method)

    def __enter__(self):
        return self.root()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class LocalSession(Session):

    __slots__ = ('lsid', 'nextloid', 'objects')

    def __init__(self, conn, lsid, root_factory=None, lformat=None):
        if lsid in conn.lsessions:
            raise RuntimeError('Session ID in use: {}'.format(lsid))

        super().__init__(conn, lformat)
        self.lsid = lsid
        self.nextloid = 0
        self.objects = {}

        conn.lsessions[lsid] = self
        try:
            # Create the root last, because it may depend on the session being
            # already set up.
            self.objects[None] = (root_factory or conn.root_factory)(self)
        except Exception:
            del conn.lsessions[lsid]
            raise

    def root(self):
        return self.objects[None]

    def add(self, lobj):
        loid = self.nextloid
        self.nextloid += 1
        self.objects[loid] = lobj
        return loid

    def unmarshal_obj(self, ref, srcid):
        oid = ref[OBJECT_ID]

        if 'lsid' in ref:
            # This is actually a RemoteObject (a callback).  Don't use a real
            # RemoteSession, because we don't manage its lifetime.
            rsession = RemoteSessionBase(self.conn, ref['lsid'], None, srcid)
            rsession.lcodec = self.lcodec
            return rsession.unmarshal_id(oid)

        lsession = self.conn.unmarshal_lsession(ref.get('rsid'))
        return lsession.unmarshal_id(oid)

    def unmarshal_id(self, loid):
        lobj = self.objects.get(loid)
        if lobj is None:
            raise LookupError('Unknown object: {}'.format(loid))
        return lobj

    # In create_bridge, default formats to json rather than None because
    # bridging peer probably doesn't need to decode and re-encode message
    # bodies.
    @coroutine
    def create_bridge(self, rconn, lformat='json', rformat='json'):
        rsession = yield from rconn.create_session(rformat=rformat)
        return Bridge(self, rsession, lformat)

    def close(self):
        self.root().release()

    def destroy(self):
        for lobj in self.objects.values():
            lobj._close()
        del self.conn.lsessions[self.lsid]

    def free(self, loid):
        lobj = self.unmarshal_id(loid)
        lobj.release()


class NativeFunction:

    def __init__(self, f):
        self.call = f


class NativeSession(LocalSession):

    __slots__ = ()

    def marshal(self, obj, method=None):
        loid = self.nextloid
        self.nextloid += 1
        self.objects[loid] = obj

        lref = {OBJECT_ID: loid, 'lsid': self.lsid}
        if method is not None:
            lref['method'] = method
        return lref

    def unmarshal_id(self, loid):
        obj = super().unmarshal_id(loid)
        return NativeFunction(obj) if isinstance(obj, FunctionType) else obj

    def destroy(self):
        del self.conn.lsessions[self.lsid]

    def free(self, loid):
        if loid is None:
            return super().free(None)  # root

        obj = self.objects.pop(loid, None)
        if obj is None:
            raise LookupError('Unknown object: {}'.format(loid))


class LocalObjectBase:

    __slots__ = ('_lsession', '_loid', '_lref', '_nref')

    def __init__(self, lsession, loid):
        self._lsession = lsession
        self._loid = loid
        self._lref = {OBJECT_ID: loid, 'lsid': lsession.lsid}

    def addref(self):
        """Increment the object's reference count."""
        self._nref += 1

    def release(self):
        """Decrement the object's reference count.  When the reference count
        reaches zero, it will be removed from memory."""
        self._nref -= 1
        if self._nref <= 0:
            self._release()

    def help(self, method=None):
        """Get documentation for the object or one of its methods."""
        return getdoc(self if method is None else method)

    def dir(self):
        """Get a list of names of the object's methods."""
        return [n for n in dir(self) if n[:1] != '_']

    def taxa(self):
        """Get a list of names of the object's base classes."""
        # exclude eider classes
        return [c.__name__ for c in self.__class__.__mro__][:-3]

    def signature(self, method):
        """Get method type signature."""
        return marshal_signature(signature(method))

    def _release(self):
        del self._lsession.objects[self._loid]
        self._close()

    def _close(self):
        pass

    def _marshal(self):
        self._nref += 1
        return self._lref

    def __enter__(self):
        self._nref += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class LocalRootType(type):
    """Automatically create new_Foo() factory functions for newable classes."""

    def __new__(cls, name, bases, names):
        def create_factory(c):
            # the class can provide its own factory function
            new = getattr(c, '_new', c)
            facname = 'new_' + c.__name__

            # Create a wrapper with a similar signature to the wrapped
            # function.  This is similar to what decorator.FunctionMaker does,
            # but simplified for our particular case.

            # Remove first parameter, and replace type hints with forward
            # references.
            sig_def = signature(new)
            ret = sig_def.return_annotation
            sig_def = sig_def.replace(
                parameters=[(p if p.annotation is Parameter.empty else
                             p.replace(annotation=marshal_annotation(
                                p.annotation)))
                            for p in sig_def.parameters.values()][1:],
                return_annotation=(ret if ret is Signature.empty else
                                   marshal_annotation(ret)))

            # Make call-site version of the signature.
            sig_call = sig_def.replace(
                parameters=[p.replace(default=Parameter.empty,
                                      annotation=Parameter.empty)
                            for p in sig_def.parameters.values()],
                return_annotation=Signature.empty)

            # Create the function.  Unfortunately there is no clean way to do
            # this without exec.
            locs = {'new': new}
            exec("""def {}(self, {}:
                        {!r}
                        return new(self._lsession, {}
                 """.format(facname, str(sig_def)[1:],
                            new.__doc__,
                            str(sig_call)[1:]),
                 locs, locs)

            return locs[facname]

        for c in names.get('_newables', []):
            factory = create_factory(c)
            names[factory.__name__] = factory

        return super().__new__(cls, name, bases, names)


class LocalRoot(LocalObjectBase, metaclass=LocalRootType):

    __slots__ = ()

    def __init__(self, lsession):
        super().__init__(lsession, None)
        # root objects are born with one implicit reference
        self._nref = 1

    def _close(self):
        self._lsession.destroy()


class LocalSessionManager(LocalRoot):

    __slots__ = ()

    def open(self, lsid, lformat=None):
        """Open a new session."""
        self._lsession.conn.create_local_session(lsid, lformat=lformat)

    def free(self, lsid, loid):
        """Release the specified object, which may be a native object."""
        lsession = self._lsession.conn.unmarshal_lsession(lsid)
        lsession.free(loid)


class LocalObject(LocalObjectBase):

    __slots__ = ()

    def __init__(self, lsession):
        super().__init__(lsession, lsession.add(self))
        # this object has no references until it is marshalled
        self._nref = 0


class RemoteMethod:

    __slots__ = ('robj', 'method')

    def __init__(self, robj, method):
        self.robj = robj
        self.method = method

    def __call__(self, *params):
        return self.robj._rsession.call(self.robj, self.method, params)

    def _marshal(self):
        return dict(self.robj._marshal(),
                    method=self.method)

    def close(self):
        return self.robj._close()

    @coroutine
    def __aenter__(self):
        return self

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        yield from self.close()

    # Synchronous context manager for Python 3.4

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def help(self):
        return self.robj.help(self)

    def signature(self):
        return self.robj.signature(self)


class RemoteObject:

    __slots__ = ('_rsession', '_rref', '_closed')

    def __init__(self, rsession, roid):
        self._rsession = rsession
        rsid = rsession.rsid
        self._rref = {OBJECT_ID: roid, 'rsid': rsid}
        self._closed = False

    def __getattr__(self, name):
        return RemoteMethod(self, name)

    def _marshal(self):
        return self._rref

    def _close(self):
        """Release the object without waiting for garbage collection.  This
        guards against double-releasing and gracefully handles dropped
        connections.  This should normally be called instead of directly
        calling release().  Despite the leading underscore in the name, client
        code may call this function.  The underscore merely exists to
        differentiate this from a remote method."""
        # If the session or the connection or the bridged connection is already
        # closed, then don't raise an error, because the remote object is
        # already dead.
        fut = Future(loop=self._rsession.conn.loop)
        if self._closed:
            # object is already closed
            fut.set_result(None)
            return fut

        if self._rsession.closed() or self._rsession.conn.closed:
            # session is already closed, or direct connection is already dead
            self._closed = True
            fut.set_result(None)
            return fut

        self._closed = True
        try:
            # calling free instead of release allows native objects to be
            # unreferenced
            did = self._rsession.call(
                None, 'free', [self._rref['rsid'], self._rref[OBJECT_ID]])
        except Exception as exc:
            # unexpected
            fut.set_exception(exc)
        else:
            def done(did):
                try:
                    did.result()
                except (LookupError, DisconnectedError):
                    # session is now closed, or connection (direct or bridged)
                    # is now dead
                    fut.set_result(None)
                except Exception as exc:
                    # unexpected
                    fut.set_exception(exc)
                else:
                    # object successfully released
                    fut.set_result(None)
            did.add_done_callback(done)
        return fut

    @coroutine
    def __aenter__(self):
        return self

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        yield from self._close()

    # Synchronous context manager for Python 3.4

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close()

    def __del__(self):
        # this can be executed in a different thread
        if not self._closed:
            loop = self._rsession.conn.loop
            if not loop.is_closed():
                loop.call_soon_threadsafe(self._close)

    def __aiter__(self):
        return RemoteIterator(self)


class RemoteIterator:

    __slots__ = ('robj', 'iter')

    def __init__(self, robj):
        self.robj = robj
        self.iter = -1

    def __aiter__(self):
        return self

    @coroutine
    def __anext__(self):
        iter = self.iter
        if iter is None:
            raise StopAsyncIteration

        if iter == -1:
            # first call
            try:
                iter = yield from self.robj.iter()
            except AttributeError:
                # fallback to sequence protocol
                iter = 0
            except Exception:
                self.iter = None
                raise
            self.iter = iter

        if isinstance(iter, int):
            # sequence protocol
            try:
                it = yield from self.robj.get(iter)
            except IndexError:
                self.iter = None
                raise StopAsyncIteration
            except Exception:
                self.iter = None
                raise
            self.iter = iter + 1
            return it

        # JavaScript-style iteration protocol: use a 'done' flag instead of
        # StopIteration
        try:
            it = yield from iter.next()
            if it.get('done'):
                raise StopAsyncIteration
        except Exception:
            self.iter = None
            yield from iter._close()
            raise
        return it['value']

    def close(self):
        iter = self.iter
        self.iter = None
        if isinstance(iter, RemoteObject):
            return iter._close()
        fut = Future(loop=self.robj._rsession.conn.loop)
        fut.set_result(None)
        return fut

    @coroutine
    def __aenter__(self):
        return self

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        yield from self.close()

    # Synchronous context manager for Python 3.4

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class RemoteCall(Future):

    def __init__(self, rsession, rcid, loop):
        super().__init__(loop=loop)
        self.rsession = rsession
        self.rcid = rcid

    def cancel(self):
        rsession = self.rsession
        rcid = self.rcid
        conn = rsession.conn
        if conn.rcalls.pop(rcid, None) is None:
            return False

        msg = {'cancel': rcid}
        dstid = rsession.dstid
        if dstid is not None:
            msg['dst'] = dstid
        conn.send(msg)
        return super().cancel()


class RemoteSessionBase(Session):

    __slots__ = ('rsid', 'dstid')

    def __init__(self, conn, rsid, lformat=None, dstid=None):
        super().__init__(conn, lformat)
        self.rsid = rsid
        self.dstid = dstid

    def call(self, robj, method, params=[]):
        conn = self.conn
        rcid = conn.nextrcid
        conn.nextrcid += 1
        conn.sendcall(self.lcodec, self.dstid, rcid, robj, method, params)

        rcall = RemoteCall(self, rcid, loop=conn.loop)
        conn.rcalls[rcid] = rcall
        return rcall

    def unmarshal_obj(self, ref, srcid):
        oid = ref[OBJECT_ID]

        if 'rsid' in ref:
            # This is actually a LocalObject (a callback) being passed back to
            # us.
            lsession = self.conn.unmarshal_lsession(ref['rsid'])
            return lsession.unmarshal_id(oid)

        rsid = ref.get('lsid')
        if rsid == self.rsid:
            rsession = self
        else:
            # This is actually a reference to an object in another session (or
            # a native object).  Don't use a real RemoteSession, because we
            # don't manage its lifetime.
            rsession = self.ExternalSession(self.conn, rsid, None, self.dstid)
            rsession.lcodec = self.lcodec

        robj = rsession.unmarshal_id(oid)

        if 'bridge' in ref:
            return rsession.unmarshal_bridge(robj, ref['bridge'])

        return robj

    def unmarshal_bridge(self, bridge, spec):
        return BridgedSession(bridge, spec)

    def unmarshal_id(self, roid):
        return RemoteObject(self, roid)

    def closed(self):
        return False


RemoteSessionBase.ExternalSession = RemoteSessionBase


class RemoteSessionManaged(RemoteSessionBase):

    __slots__ = ('_root',)

    def __init__(self, conn, rsid, lformat=None, dstid=None):
        super().__init__(conn, rsid, lformat, dstid)
        self._root = self.unmarshal_id(None)

    def root(self):
        return self._root

    def closed(self):
        return self._root._closed

    @coroutine
    def __aenter__(self):
        return self.root()

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        yield from self.close()


class RemoteSession(RemoteSessionManaged):

    __slots__ = ()

    def close(self):
        return self._root._close()


class BridgedSession(RemoteSessionManaged):

    __slots__ = ('bridge',)

    def __init__(self, bridge, spec):
        super().__init__(bridge._rsession.conn, spec['rsid'], spec['lformat'],
                         spec['dst'])
        self.bridge = bridge

        # the bridge automatically closes the session for us
        self._root._closed = True

    def close(self):
        return self.bridge._close()

    def closed(self):
        return self.bridge._closed


class Bridge(LocalObject):
    """Bridge between clients.  Allows one client to call methods exposed by
    another client."""

    __slots__ = ('_rsession',)

    def __init__(self, lsession, rsession, lformat):
        super().__init__(lsession)
        self._rsession = rsession
        self._lref['bridge'] = {'dst': rsession.conn.id,
                                'rsid': rsession.rsid,
                                'lformat': lformat}

    def _close(self):
        self._rsession.close()


class Registry:

    def __init__(self):
        self.objects = {}
        self.nextid = 0

    def add(self, obj):
        id = self.nextid
        self.objects[id] = obj
        self.nextid += 1
        return id

    def get(self, id):
        return self.objects.get(id)

    def remove(self, id):
        self.objects.pop(id, None)


class Connection:

    _thread_local = local()

    def __init__(self, whither, loop=None, *, root=LocalRoot,
                 lformat='json', rformat='json', rformat_bin='msgpack',
                 logger=None, registry=None, ws_lib=WS_LIB_DEFAULT):
        self.root_factory = root
        self.loop = loop or get_event_loop()
        self.logger = logger or getLogger('eider')
        self.lencode = Codec.registry[lformat].encode
        self.rcodec = Codec.registry[rformat]
        self.rcodec_bin = Codec.registry.get(rformat_bin)
        self.ws_lib = ws_lib

        # connection state
        self.lsessions = {}  # local sessions
        self.lcalls = {}  # local calls
        self.rcalls = {}  # remote calls
        self.bcalls = defaultdict(set)  # bridged calls
        self.nextlsid = -2  # next local session id
        self.nextrsid = 0  # next remote session id
        self.nextrcid = 0  # next remote call id

        LocalSession(self, None, LocalSessionManager)  # root session
        NativeSession(self, -1, LocalRoot)  # native (non-LocalObject) session

        if registry is None:
            registry = getattr(self._thread_local, 'registry', None)
            if registry is None:
                self._thread_local.registry = registry = Registry()
        self.registry = registry
        self.id = registry.add(self)

        self.ws = None
        self.sendq = Queue(loop=self.loop)
        self.opened = Future(loop=self.loop)
        self.task = self.loop.create_task(self.receive(whither))
        self.closed = False

    @coroutine
    def wait_opened(self):
        opened = yield from self.opened
        if self.closed:
            derr = DisconnectedError(
                'Connection closed' if opened else 'Could not connect')
            # Finalize the receive task, so the user doesn't have to also call
            # wait_closed().
            try:
                yield from self.task
            except Exception as exc:
                raise derr from exc
            raise derr

    def create_local_session(self, lsid=None, root_factory=None, lformat=None):
        if lsid is None:
            lsid = self.nextlsid
            self.nextlsid -= 1
        return LocalSession(self, lsid, root_factory, lformat)

    @coroutine
    def _create_session(self, lformat=None, rformat=None):
        rsid = self.nextrsid
        self.nextrsid += 1
        session = RemoteSession(self, rsid, lformat)
        yield from session.call(None, 'open', [rsid, rformat])
        return session

    if version_info[:2] < (3, 5):
        create_session = _create_session
    else:
        def create_session(self, lformat=None, rformat=None):
            return CoroutineContextManager(
                self._create_session(lformat, rformat))

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.registry.remove(self.id)
        self.task.cancel()

    @coroutine
    def wait_closed(self):
        yield from self.task

    @coroutine
    def __aenter__(self):
        yield from self.wait_opened()
        return self

    @coroutine
    def __aexit__(self, exc_type, exc_value, traceback):
        self.close()
        yield from self.task

    @coroutine
    def receive(self, whither):
        http_session = None
        close_code = WSCloseCode.OK
        send_task = None
        try:
            if isinstance(whither, str):
                if self.ws_lib == 'websockets':
                    self.ws = yield from ws_connect(whither, loop=self.loop)
                else:
                    http_session = ClientSession(loop=self.loop)
                    self.ws = yield from http_session.ws_connect(whither)
            else:
                self.ws = whither

            if isinstance(self.ws, WebSocketCommonProtocol):
                self.ws_send = self.ws.send
                ws_recv = self.ws_recv_websockets
            else:
                self.ws_send = self.ws_send_aiohttp
                ws_recv = self.ws_recv_aiohttp

            self.opened.set_result(True)

            send_task = self.loop.create_task(self.send_forever())

            header = None
            while 1:
                tp, data = yield from ws_recv()

                if tp in (WSMsgType.TEXT, WSMsgType.BINARY):
                    self.log_data('recv', data)
                    if header is None:
                        rcodec = (self.rcodec if tp == WSMsgType.TEXT else
                                  self.rcodec_bin)
                        try:
                            msg = rcodec.decode(data)
                        except Exception as exc:
                            close_code = WSCloseCode.POLICY_VIOLATION
                            raise ProtocolError(
                                'Invalid data received on Eider WebSocket '
                                'connection: {}: {}'
                                .format(type(exc).__name__, exc))

                        if msg.get('format') is None:
                            self.dispatch(rcodec, msg)
                        else:
                            header = msg
                    else:
                        self.dispatch(rcodec, header, data)
                        header = None

                elif tp in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
                elif tp == WSMsgType.ERROR:
                    close_code = getattr(data, 'code',
                                         WSCloseCode.INTERNAL_ERROR)
                    raise ProtocolError('Eider WebSocket error: {}: {}'.format(
                        type(data).__name__, data))
                else:
                    close_code = WSCloseCode.UNSUPPORTED_DATA
                    raise ProtocolError(
                        'Received invalid Eider WebSocket message type {}'
                        .format(tp))

        except CancelledError:
            close_code = WSCloseCode.GOING_AWAY
            # If we got here because close() was called, we should swallow the
            # exception to avoid cancelling an outer task.  If close() was not
            # called, then we got here because an outer task was cancelled, and
            # we should not swallow it.
            if not self.closed:
                raise
        except ProtocolError:
            raise
        except ConnectionClosed:
            pass
        except Exception:
            close_code = WSCloseCode.INTERNAL_ERROR
            raise
        finally:
            if not self.opened.done():
                self.opened.set_result(False)
            self.closed = True
            self.lclose()
            self.rclose()
            if send_task is not None:
                send_task.cancel()
                try:
                    yield from send_task
                except CancelledError:
                    pass
                except Exception as exc:
                    self.logger.error('{}: {}'.format(type(exc).__name__, exc),
                                      exc_info=True)
            if self.ws is not None:
                yield from self.ws.close(code=close_code)
            if http_session is not None:
                yield from http_session.close()

    def dispatch(self, rcodec, header, body=None):
        dstid = header.get('dst')
        method = header.get('method')
        if method:
            # this is a call
            cid = header.get('id')
            if dstid is None:
                srcid = header.get('src')
                try:
                    if method[:1] == '_':
                        raise AttributeError(
                            "Cannot call private method '{}'".format(method))

                    if body is None:
                        msg = header
                    else:
                        rcodec = Codec.byname(header['format'])
                        msg = rcodec.decode(body)

                    result = self.apply_begin(rcodec, srcid, method, msg)
                except Exception as exc:
                    self.on_error(srcid, cid, exc)
                else:
                    if isinstance(result, Future):
                        # A subclass might override apply_begin() to execute
                        # asynchronously.
                        if cid is not None:
                            # mark the call as received but not yet applied
                            self.lcalls[cid] = False
                        result.add_done_callback(
                            partial(self.on_apply_begin_done, srcid, cid))
                    else:
                        lsession, loid = result
                        lcodec = lsession.lcodec
                        try:
                            result = self.apply_finish(
                                rcodec, srcid, method, lsession, loid, msg)
                        except Exception as exc:
                            self.on_error(srcid, cid, exc, lcodec)
                        else:
                            self.on_applied(srcid, cid, result, lcodec)
            else:
                self.bridge_call(dstid, cid, header, body)
        else:
            cancelid = header.get('cancel')
            if cancelid is not None:
                # this is a cancel request
                if dstid is None:
                    lcall = self.lcalls.get(cancelid)
                    if lcall is not None:
                        if not lcall:
                            # mark the call for cancellation by on_applied()
                            self.lcalls[cancelid] = True
                        else:
                            self.lcalls.pop(cancelid, None)
                            lcall.cancel()
                else:
                    self.bridge_call(dstid, None, header, body)
            else:
                # this is a response
                cid = header.get('id')
                if dstid is None:
                    rcall = self.rcalls.pop(cid, None)
                    if rcall is not None:
                        try:
                            if body is None:
                                msg = header
                            else:
                                rcodec = Codec.byname(header['format'])
                                msg = rcodec.decode(body)

                            result = self.getresult(rcodec, rcall.rsession,
                                                    msg)
                        except Exception as exc:
                            rcall.set_exception(exc)
                        else:
                            rcall.set_result(result)
                else:
                    self.bridge_response(dstid, cid, header, body)

    def bridge_call(self, dstid, cid, header, body):
        dst = self.registry.get(dstid)
        if dst is None:
            self.on_error(None, cid, DisconnectedError(
                'Unknown connection: {}'.format(dstid)))
        else:
            # forward message to intended callee
            del header['dst']  # no further forwarding
            header['src'] = self.id  # tell callee where to send the response
            dst.send(header, body)

            if cid is not None:
                dst.bcalls[self.id].add(cid)

    def bridge_response(self, dstid, cid, header, body):
        dst = self.registry.get(dstid)
        if dst is not None:
            self.bcalls[dstid].discard(cid)

            # forward response to caller
            del header['dst']  # no further forwarding
            dst.send(header, body)

    def unmarshal_lsession(self, lsid):
        lsession = self.lsessions.get(lsid)
        if lsession is None:
            raise LookupError('Unknown session: {}'.format(lsid))
        return lsession

    def apply_begin(self, rcodec, srcid, method, msg):
        lref = msg.get('this')
        if lref is None:
            loid = None
            lsid = None
        else:
            if isinstance(lref, Reference):
                lref = lref.ref
            if isinstance(lref, dict):
                loid = lref.get(OBJECT_ID)
                lsid = lref.get('rsid')
            else:
                raise TypeError('Malformed this object')

        return self.unmarshal_lsession(lsid), loid

    def apply_finish(self, rcodec, srcid, method, lsession, loid, msg):
        lobj = lsession.unmarshal_id(loid)
        params = lsession.unmarshal_all(rcodec, msg.get('params', []), srcid)

        try:
            a = getattr(lobj, method)
        except AttributeError:
            if method[:4] == 'set_' and len(params) == 1:
                # direct property assignment
                name = method[4:]
                if name[:1] == '_':
                    raise AttributeError(
                        "Cannot assign to private attribute '{}'".format(name))
                try:
                    a = getattr(lobj, name)
                except AttributeError:
                    pass
                else:
                    if callable(a):
                        raise AttributeError(
                            "Cannot assign to method '{}'".format(name))
                setattr(lobj, name, params[0])
                return
            raise

        if not callable(a) and not params:
            # direct property access
            return a

        # method call
        return a(*params)

    def getresult(self, rcodec, rsession, msg):
        if 'result' in msg:
            return rsession.unmarshal_all(rcodec, msg['result'])
        else:
            if 'error' in msg:
                # attempt to unmarshal error object; fallback to generic
                # Exception
                error = msg['error']
                message = str(error.get('message', '')) or 'Unspecified error'
                name = str(error.get('name'))
                etype = globals().get(name) or getattr(builtins, name, None)
                if not (isinstance(etype, type) and
                        issubclass(etype, Exception)):
                    etype = Exception
                    if name:
                        message = '{}: {}'.format(name, message)
                stack = error.get('stack', '')
                if stack:
                    # rearrange newlines to make tracebacks prettier
                    stack = '\n' + stack.rstrip('\n')
                raise etype(message) from RemoteError(stack)
            raise Exception('Unspecified error')

    def on_apply_begin_done(self, srcid, lcid, fut):
        try:
            lcodec, fut = fut.result()
        except Exception as exc:
            self.on_error(srcid, lcid, exc)
        else:
            fut.add_done_callback(
                partial(self.on_apply_finish_done, srcid, lcid, lcodec))

    def on_apply_finish_done(self, srcid, lcid, lcodec, fut):
        try:
            result = fut.result()
        except Exception as exc:
            self.on_error(srcid, lcid, exc, lcodec)
        else:
            self.on_applied(srcid, lcid, result, lcodec)

    def on_applied(self, srcid, lcid, result, lcodec):
        if iscoroutine(result):
            # A method may be a coroutine ...
            result = self.loop.create_task(result)
        if isinstance(result, Future):
            # ... or it may return a Future ...
            if lcid is not None:
                if self.lcalls.pop(lcid, None):
                    # cancel was already requested
                    result.cancel()
                else:
                    self.lcalls[lcid] = result
            result.add_done_callback(
                partial(self.on_done, srcid, lcid, lcodec))
        else:
            # ... or it may return a simple value.
            self.lcalls.pop(lcid, None)
            self.on_result(srcid, lcid, result, lcodec)

    def on_error(self, srcid, lcid, exc, lcodec=None):
        if lcid is None:
            self.logger.error('{}: {}'.format(type(exc).__name__, exc),
                              exc_info=(type(exc), exc, exc.__traceback__))
        else:
            self.error(srcid, lcid, exc, lcodec)

    def on_result(self, srcid, lcid, result, lcodec):
        if lcid is not None:
            self.respond(srcid, lcid, result, lcodec)

    def on_done(self, srcid, lcid, lcodec, fut):
        self.lcalls.pop(lcid, None)
        try:
            result = fut.result()
        except Exception as exc:
            self.on_error(srcid, lcid, exc, lcodec)
        else:
            self.on_result(srcid, lcid, result, lcodec)

    def lclose(self):
        while self.lsessions:
            next(iter(self.lsessions.values())).objects[None]._release()

    def rclose(self):
        self.registry.remove(self.id)

        # cancel any outstanding local calls
        for lcall in self.lcalls.values():
            if lcall:
                lcall.cancel()

        # dispose of outstanding remote calls
        while self.rcalls:
            rcid, rcall = self.rcalls.popitem()
            rcall.set_exception(DisconnectedError('Connection lost'))

        # dispose of outstanding bridged calls (as callee)
        while self.bcalls:
            srcid, cids = self.bcalls.popitem()
            src = self.registry.get(srcid)
            if src is not None:
                for cid in cids:
                    src.error(None, cid,
                              DisconnectedError('Bridged connection lost'))

        # dispose of outstanding bridged calls (as caller)
        for conn in self.registry.objects.values():
            cids = conn.bcalls.pop(self.id, [])
            if not conn.closed:
                for cid in cids:
                    conn.send({'cancel': cid})

    def sendcall(self, lcodec, dstid, rcid, robj, method, params=[]):
        if self.closed:
            raise DisconnectedError('Connection closed')

        header = {'id': rcid, 'method': method}
        if dstid is not None:
            header['dst'] = dstid
        if lcodec is None:
            body = None
            if robj is not None:
                header['this'] = robj
            if params:
                header['params'] = params
        else:
            body = {}
            if robj is not None:
                body['this'] = robj
            if params:
                body['params'] = params
            body = lcodec.encode(self, body)
            header['format'] = lcodec.name
        self.send(header, body)

    def respond(self, srcid, lcid, result, lcodec):
        header = {'id': lcid}
        if srcid is not None:
            header['dst'] = srcid
        if lcodec is None:
            body = None
            header['result'] = result
        else:
            try:
                body = lcodec.encode(self, {'result': result})
            except Exception as exc:
                self.error(srcid, lcid, exc, lcodec)
                return
            header['format'] = lcodec.name
        self.send(header, body)

    def error(self, srcid, lcid, exc, lcodec=None):
        header = {'id': lcid}
        if srcid is not None:
            header['dst'] = srcid
        tb = StringIO()
        etype = type(exc)
        print_exception(etype, exc, exc.__traceback__, file=tb)
        error = {'name': etype.__name__,
                 'message': str(exc),
                 'stack': tb.getvalue()}
        if lcodec is None:
            body = None
            header['error'] = error
        else:
            try:
                body = lcodec.encode(self, {'error': error})
            except Exception:
                body = None
                header['error'] = error
            else:
                header['format'] = lcodec.name
        self.send(header, body)

    def send(self, header, body=None):
        if not self.closed:
            self.sendq.put_nowait(self.lencode(self, header))
            if body is not None:
                self.sendq.put_nowait(body)

    @coroutine
    def send_forever(self):
        while 1:
            data = yield from self.sendq.get()
            self.log_data('send', data)
            try:
                yield from self.ws_send(data)
            except CancelledError:
                raise
            except Exception as exc:
                self.logger.error(
                    '{}: {}'.format(type(exc).__name__, exc), exc_info=True)

    @coroutine
    def ws_send_aiohttp(self, data):
        if isinstance(data, str):
            yield from self.ws.send_str(data)
        else:
            yield from self.ws.send_bytes(data)

    @coroutine
    def ws_recv_aiohttp(self):
        tp, data, extra = yield from self.ws.receive()
        return tp, data

    @coroutine
    def ws_recv_websockets(self):
        data = yield from self.ws.recv()
        if isinstance(data, str):
            tp = WSMsgType.TEXT
        else:
            tp = WSMsgType.BINARY
        return tp, data

    def log_data(self, tag, data):
        logger = self.logger
        if logger.isEnabledFor(DEBUG):
            if isinstance(data, str):
                t = 'str'
                n = len(data.encode())
                s = data
            else:
                t = 'bin'
                n = len(data)
                s = b64encode(data).decode()
            logger.debug('{} {} {:3} {}'.format(tag, t, n, s[:512]))


if platform == 'win32':
    # Wake up the event loop once every second to allow Ctrl+C to get through.
    # http://stackoverflow.com/questions/27480967/why-does-the-asyncios-event-loop-suppress-the-keyboardinterrupt-on-windows
    def enable_ctrl_c(loop):
        loop.call_later(1, enable_ctrl_c, loop)
else:
    def enable_ctrl_c(loop):
        pass


class BlockingMethod(RemoteMethod):

    __slots__ = ()

    def __call__(self, *params):
        return self.robj._rsession.conn.loop.run_until_complete(
            super().__call__(*params))


class BlockingObject(RemoteObject):

    __slots__ = ()

    def _close(self):
        self._rsession.conn.loop.run_until_complete(super()._close())

    def __del__(self):
        # Follow the same pattern as BlockingConnection._close(); see comments
        # there.
        try:
            if (self._closed or self._rsession.closed() or
                    self._rsession.conn.closed or
                    self._rsession.conn.loop.is_closed()):
                return

            if self._rsession.conn.loop.is_running():
                self._rsession.conn.loop.call_soon_threadsafe(super()._close)
            else:
                self._close()
        except Exception:
            pass

    def __getattr__(self, name):
        return BlockingMethod(self, name)

    def __setattr__(self, name, value):
        """syntactic sugar: 'obj.prop = val' --> 'obj.set_prop(val)'"""
        if name[:1] == '_':
            super().__setattr__(name, value)
        else:
            BlockingMethod(self, 'set_' + name)(value)

    def __len__(self):
        """syntactic sugar: 'len(obj)' --> 'obj.length()'"""
        return self.length()

    def __getitem__(self, key):
        """syntactic sugar: 'obj[key]' --> 'obj.get(key)'"""
        return self.get(key)

    def __setitem__(self, key, val):
        """syntactic sugar: 'obj[key] = val' --> 'obj.set(key, val)'"""
        return self.set(key, val)

    def __delitem__(self, key):
        """syntactic sugar: 'del obj[key]' --> 'obj.remove(key)'"""
        return self.remove(key)

    def __iter__(self):
        """syntactic sugar: 'iter(obj)' --> 'obj.iter()' (with fallback to
        sequence protocol)"""
        try:
            return self.iter()
        except AttributeError:
            return SequenceIterator(self)

    def __next__(self):
        """syntactic sugar: 'next(obj)' --> 'obj.next()'"""
        # JavaScript-style iteration protocol: use a 'done' flag instead of
        # StopIteration
        it = self.next()
        if it.get('done'):
            raise StopIteration
        return it['value']


class SequenceIterator:
    """Emulates Python's built-in iteration of sequence-like objects."""

    __slots__ = ('seq', 'i')

    def __init__(self, seq):
        self.seq = seq
        self.i = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.i < 0:
            raise StopIteration

        try:
            it = self.seq[self.i]
        except IndexError:
            self.i = -1
            raise StopIteration

        self.i += 1
        return it


class BlockingSessionMixin:

    __slots__ = ()

    def unmarshal_bridge(self, bridge, spec):
        return BlockingBridgedSession(bridge, spec)

    def unmarshal_id(self, roid):
        return BlockingObject(self, roid)


class BlockingExternalSession(BlockingSessionMixin, RemoteSessionBase):

    __slots__ = ()


class BlockingSession(BlockingSessionMixin, RemoteSession):

    __slots__ = ()

    ExternalSession = BlockingExternalSession


class BlockingBridgedSession(BlockingSessionMixin, BridgedSession):

    __slots__ = ()


class BlockingConnection:

    def __init__(self, url='ws://localhost:8080/', loop=None, **kwargs):
        if loop is None:
            loop = get_event_loop()
        enable_ctrl_c(loop)
        self.conn = Connection(url, loop, **kwargs)
        loop.run_until_complete(self.conn.wait_opened())

    def close(self):
        try:
            # Bail out as quickly as possible if we're already closed.
            if self.conn.closed:
                return

            self.conn.close()

            # We could be currently inside an event loop, if this object was
            # abandoned and is now being garbage-collected.  If so, we don't
            # want to reentrantly run the loop, because that would cause
            # problems for the outer loop.  But if not, we want to wait for the
            # connection to properly close.
            if not self.conn.loop.is_running():
                self.conn.loop.run_until_complete(self.conn.wait_closed())
        except Exception:
            # Lots of reasons we could get an exception - the loop could be
            # already closed during garbage collection, or the connection could
            # have already failed (which is why we're closing it!).  Any
            # consequential errors should be visible elsewhere to code that was
            # using the connection.  So we swallow exceptions here to avoid
            # cluttering logs with redundant error messages.
            pass

    # If this object is abandoned, make sure to close the underlying
    # asynchronous Connection; otherwise the Connection.receive() task will
    # stay active.
    __del__ = close

    def create_local_session(self, *args, **kwargs):
        return self.conn.create_local_session(*args, **kwargs)

    def create_session(self, lformat=None, rformat=None):
        rsid = self.conn.nextrsid
        self.conn.nextrsid += 1
        session = BlockingSession(self.conn, rsid, lformat)
        self.conn.loop.run_until_complete(
            session.call(None, 'open', [rsid, rformat]))
        return session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


@coroutine
def receive(url='ws://localhost:8080/', loop=None, **kwargs):
    conn = Connection(url, loop, **kwargs)
    yield from conn.wait_closed()


def serve_aiohttp(port=8080, loop=None, handle_signals=True, **kwargs):
    if loop is None:
        loop = get_event_loop()
    set_event_loop(loop)

    conns = []

    @coroutine
    def handle(request):
        ws = WebSocketResponse()
        yield from ws.prepare(request)

        conn = Connection(ws, loop, **kwargs)
        conns.append(conn)
        try:
            yield from conn.wait_closed()
        finally:
            conns.remove(conn)

        return ws

    @coroutine
    def on_shutdown(app):
        for conn in conns:
            conn.close()

    app = Application()
    app.router.add_route('GET', '/', handle)
    app.on_shutdown.append(on_shutdown)

    enable_ctrl_c(loop)

    kwargs_run = {}
    aiohttp_ver = tuple(map(int, aiohttp_version.split('.')))
    if aiohttp_ver < (3,):
        kwargs_run['loop'] = loop
    run_app(app, port=port, handle_signals=handle_signals, **kwargs_run)


def serve_websockets(port=8080, loop=None, handle_signals=True, **kwargs):
    if loop is None:
        loop = get_event_loop()
    set_event_loop(loop)

    @coroutine
    def handle(ws, path):
        conn = Connection(ws, loop, **kwargs)
        yield from conn.wait_closed()

    enable_ctrl_c(loop)

    server = loop.run_until_complete(ws_serve(handle, 'localhost', port))
    try:
        loop.run_forever()
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())


def serve(port=8080, loop=None, handle_signals=True, ws_lib=WS_LIB_DEFAULT,
          **kwargs):
    serve = serve_websockets if ws_lib == 'websockets' else serve_aiohttp
    serve(port, loop, handle_signals, **kwargs)

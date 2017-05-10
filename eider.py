#   Copyright 2017 Semiconductor Components Industries LLC (d/b/a "ON Semiconductor")
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
    eider
    ~~~~~

    The Eider RPC protocol.
"""

from asyncio import CancelledError, coroutine, Future, get_event_loop, iscoroutine
import builtins
from collections import defaultdict
from functools import partial
from inspect import getdoc, Parameter, signature, Signature
from io import StringIO
from json import dumps, loads as decode
from logging import getLogger
from threading import local
from traceback import print_exception
from types import FunctionType, MethodType
from weakref import WeakValueDictionary

from aiohttp import __version__ as aiohttp_version, ClientSession, WSCloseCode, WSMsgType
from aiohttp.web import Application, run_app, WebSocketResponse

__version__ = '0.9.4'

try:
    StopAsyncIteration = builtins.StopAsyncIteration  # added in Python 3.5
except AttributeError:
    class StopAsyncIteration(Exception):
        pass

@coroutine
def async_for(it, body, else_=None):
    """Simulate 'async for' in Python 3.4."""
    iter = type(it).__aiter__(it)
    anext = type(iter).__anext__
    running = True
    while running:
        try:
            x = yield from anext(iter)
        except StopAsyncIteration:
            running = False
        else:
            if (yield from body(x)):
                break
    else:
        if else_ is not None:
            yield from else_()

OBJECT_ID = '__*__'  # Marker for marshallable object references within encoded data

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
        return packb(data, use_bin_type=True, default=partial(msgpack_default, conn))
    
    def msgpack_ext_hook(code, data):
        if code == 0:
            return Reference(unpackb(data, encoding='utf-8'))
        return ExtType(code, data)
    
    def msgpack_decode(data):
        return unpackb(data, encoding='utf-8', ext_hook=msgpack_ext_hook)
    
    Codec('msgpack', msgpack_encode, msgpack_decode, False)

def marshal_annotation(annot):
    # cf. inspect.formatannotation
    if isinstance(annot, type):
        r = repr(annot)  # give the class's __repr__ a chance to give us something eval-able
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
             None if param.annotation is Parameter.empty else marshal_annotation(param.annotation))
                for name, param in params.items()
                    if param.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_POSITIONAL)
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
                     return_annotation=Signature.empty if ret is None else
                        unmarshal_annotation(ret))

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
            return {k: self.unmarshal_all_inband(o, srcid) for k, o in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self.unmarshal_all_inband(o, srcid) for o in obj]
        return obj
    
    def unmarshal_all_outofband(self, obj, srcid=None):
        if isinstance(obj, dict):
            return {k: self.unmarshal_all_outofband(o, srcid) for k, o in obj.items()}
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
            raise AttributeError("Cannot access private attribute '{}' of '{}' object".format(
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
            # create the root last, because it may depend on the session being already set up
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
            # this is actually a RemoteObject (a callback)
            # don't use a real RemoteSession, because we don't manage its lifetime
            rsession = RemoteSessionBase(self.conn, ref['lsid'], None, srcid)
            rsession.lcodec = self.lcodec
            robj = rsession.unmarshal_id(oid)
            robj._closed = True  # callbacks do not need to be released
            return robj
        
        lsid = ref.get('rsid')
        lsession = self.conn.lsessions.get(lsid)
        if lsession is None:
            raise LookupError('Unknown session: {}'.format(lsid))
        
        return lsession.unmarshal_id(oid)
    
    def unmarshal_id(self, loid):
        lobj = self.objects.get(loid)
        if lobj is None:
            raise LookupError('Unknown object: {}'.format(loid))
        return lobj
    
    def close(self):
        self.root().release()

class NativeFunction:
    
    def __init__(self, f):
        self.call = f

class NativeLocalSession(LocalSession):
    
    __slots__ = ('refs',)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refs = WeakValueDictionary()
    
    def marshal(self, obj, method):
        loid = self.nextloid
        self.nextloid += 1
        self.refs[loid] = obj
        
        return {OBJECT_ID: loid,
                'lsid': self.lsid,
                'method': method}
    
    def unmarshal_id(self, loid):
        if loid is None:
            return self.objects[None]  # root
        
        obj = self.refs.get(loid)
        if obj is None:
            raise LookupError('Unknown object: {} (may have been garbage collected)'.format(loid))
        
        if isinstance(obj, FunctionType):
            return NativeFunction(obj)
        return obj

class LocalObjectBase:
    
    __slots__ = ('_lsession', '_loid', '_lref', '_nref')
    
    def __init__(self, lsession, loid):
        self._lsession = lsession
        self._loid = loid
        self._lref = {OBJECT_ID: loid, 'lsid': lsession.lsid}
        self._nref = 1
    
    def addref(self):
        """Increment the object's reference count."""
        self._nref += 1
    
    def release(self):
        """Decrement the object's reference count.  When the reference count reaches zero, it will
        be removed from memory."""
        self._nref -= 1
        if not self._nref:
            self._release()
    
    def help(self, method=None):
        """Get documentation for the object or one of its methods."""
        return getdoc(self if method is None else method)
    
    def dir(self):
        """Get a list of names of the object's methods."""
        return [n for n in dir(self) if n[:1] != '_']
    
    def taxa(self):
        """Get a list of names of the object's base classes."""
        return [c.__name__ for c in self.__class__.__mro__][:-3]  # exclude eider classes
    
    def signature(self, method):
        """Get method type signature."""
        return marshal_signature(signature(method))
    
    def _release(self):
        del self._lsession.objects[self._loid]
        self._close()
    
    def _close(self):
        pass
    
    def _marshal(self):
        return self._lref
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

class LocalRootType(type):
    """Automatically create new_Foo() factory functions for newable classes."""
    
    def __new__(cls, name, bases, names):
        def create_factory(c):
            new = getattr(c, '_new', c)  # the class can provide its own factory function
            facname = 'new_' + c.__name__
            
            # Create a wrapper with a similar signature to the wrapped function.  This is similar
            # to what decorator.FunctionMaker does, but simplified for our particular case.
            
            # Remove first parameter, and replace type hints with forward references.
            sig_def = signature(new)
            ret = sig_def.return_annotation
            sig_def = sig_def.replace(
                parameters=[(p if p.annotation is Parameter.empty else
                                    p.replace(annotation=marshal_annotation(p.annotation)))
                                for p in sig_def.parameters.values()][1:],
                return_annotation=ret if ret is Signature.empty else marshal_annotation(ret))
            
            # Make call-site version of the signature.
            sig_call = sig_def.replace(
                parameters=[p.replace(default=Parameter.empty,
                                      annotation=Parameter.empty)
                                for p in sig_def.parameters.values()],
                return_annotation=Signature.empty)
            
            # Create the function.  Unfortunately there is no clean way to do this without exec.
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
    
    def _close(self):
        # close all objects in session
        for lobj in self._lsession.objects.values():
            lobj._close()
        
        # remove session
        del self._lsession.conn.lsessions[self._lsession.lsid]

class LocalSessionManager(LocalRoot):
    
    __slots__ = ()
    
    def open(self, lsid, lformat=None):
        """Open a new session."""
        self._lsession.conn.create_local_session(lsid, lformat=lformat)

class LocalObject(LocalObjectBase):
    
    __slots__ = ()
    
    def __init__(self, lsession):
        super().__init__(lsession, lsession.add(self))

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
    
    def help(self):
        return self.robj.help(self)
    
    def signature(self):
        return self.robj.signature(self)

class RemoteObject:
    
    __slots__ = ('_rsession', '_rref', '_closed')
    
    def __init__(self, rsession, roid):
        self._rsession = rsession
        self._rref = {OBJECT_ID: roid, 'rsid': rsession.rsid}
        self._closed = False
    
    def __getattr__(self, name):
        return RemoteMethod(self, name)
    
    def _marshal(self):
        return self._rref
    
    def _close(self):
        """Release the object without waiting for garbage collection.  This guards against double-
        releasing and gracefully handles dropped connections.  This should normally be called
        instead of directly calling release().  Despite the leading underscore in the name, client
        code may call this function.  The underscore merely exists to differentiate this from a
        remote method."""
        # If the session or the connection or the bridged connection is already closed, then don't
        # raise an error, because the remote object is already dead.
        fut = Future(loop=self._rsession.conn.loop)
        if self._closed or self._rsession.closed:
            fut.set_result(None)  # already closed
        else:
            self._closed = True
            try:
                did = self._rsession.call(self._rref, 'release')
            except DisconnectedError:
                fut.set_result(None)  # direct connection is already dead
            except Exception as exc:
                fut.set_exception(exc)
            else:
                def done(did):
                    try:
                        did.result()
                    except DisconnectedError:
                        fut.set_result(None)  # connection (direct or bridged) is now dead
                    except Exception as exc:
                        fut.set_exception(exc)
                    else:
                        fut.set_result(None)  # object successfully released
                did.add_done_callback(done)
        return fut
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._close()
    
    __del__ = _close
    
    def __aiter__(self):
        return RemoteIterator(self)

class RemoteIterator:
    
    __slots__ = ('robj', 'iter')
    
    def __init__(self, robj):
        self.robj = robj
        self.iter = robj.iter()
    
    def __aiter__(self):
        return self
    
    @coroutine
    def __anext__(self):
        iter = self.iter
        if iter is None:
            raise StopAsyncIteration
        
        if isinstance(iter, Future):
            # first call
            try:
                iter = yield from iter
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
        
        # JavaScript-style iteration protocol: use a 'done' flag instead of StopIteration
        try:
            it = yield from iter.next()
            if it.get('done'):
                raise StopAsyncIteration
        except Exception:
            self.iter = None
            iter._close()
            raise
        return it['value']

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
        
        conn.send({'dst': rsession.dstid,
                   'cancel': rcid})
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
            # this is actually a LocalObject (a callback) being passed back to us
            lsid = ref['rsid']
            lsession = self.conn.lsessions.get(lsid)
            if lsession is None:
                raise LookupError('Unknown session: {}'.format(lsid))
            return lsession.unmarshal_id(oid)
        
        rsid = ref.get('lsid')
        if rsid != self.rsid:
            raise LookupError('Unknown session: {}'.format(rsid))
        
        robj = self.unmarshal_id(oid)
        
        if 'bridge' in ref:
            return self.unmarshal_bridge(robj, ref['bridge'])
        
        return robj
    
    def unmarshal_bridge(self, bridge, spec):
        return BridgedSession(bridge, spec)
    
    def unmarshal_id(self, roid):
        return RemoteObject(self, roid)

class RemoteSessionManaged(RemoteSessionBase):
    
    __slots__ = ('closed', '_root')
    
    def __init__(self, conn, rsid, lformat=None, rformat=None, dstid=None):
        super().__init__(conn, rsid, lformat, dstid)
        self.closed = True  # set temporarily in case _open() raises
        
        # For efficiency, we want to allow the session to be used without having to wait first to
        # see if the call to open it was successful.  Pretty much the only way this call can fail
        # is if the connection is closed.  And any subsequent uses of the session will fail loudly
        # anyway, so we can safely swallow any exceptions here.
        fut = self._open(rformat)
        if fut is not None:
            def done(did):
                try:
                    did.result()
                except Exception:
                    pass
            fut.add_done_callback(done)
        
        self.closed = False
        self._root = self.unmarshal_id(None)
    
    def root(self):
        return self._root
    
    def close(self):
        if not self.closed:
            self.closed = True
            self._close()
    
    __del__ = close
    
    @coroutine
    def __aenter__(self):
        return self._root
    
    @coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

class RemoteSession(RemoteSessionManaged):
    
    __slots__ = ()
    
    def __init__(self, conn, lformat=None, rformat=None):
        rsid = conn.nextrsid
        conn.nextrsid += 1
        super().__init__(conn, rsid, lformat, rformat)
    
    def _open(self, rformat):
        return self.call(None, 'open', [self.rsid, rformat])
    
    def _close(self):
        return self._root._close()

class BridgedSession(RemoteSessionManaged):
    
    __slots__ = ('bridge',)
    
    def __init__(self, bridge, spec):
        super().__init__(bridge._rsession.conn, spec['rsid'], spec['lformat'], dstid=spec['dst'])
        self.bridge = bridge
    
    def _open(self, rformat):
        pass  # the bridge automatically opens the session for us
    
    def _close(self):
        return self.bridge._close()

class Bridge(LocalObject):
    """Bridge between clients.  Allows one client to call methods exposed by another client."""
    
    __slots__ = ('_rsession',)
    
    def __init__(self, lsession, rconn,
                 # Default formats to json rather than None because bridging peer probably doesn't
                 # need to decode and re-encode message bodies.
                 lformat='json', rformat='json'):
        super().__init__(lsession)
        self._rsession = RemoteSession(rconn, rformat=rformat)
        self._lref['bridge'] = {'dst': rconn.id, 'rsid': self._rsession.rsid, 'lformat': lformat}
    
    def _close(self):
        self._rsession.close()

class Registry:
    
    def __init__(self):
        self.objects = WeakValueDictionary()
        self.nextid = 0
    
    def add(self, obj):
        id = self.nextid
        self.objects[id] = obj
        self.nextid += 1
        return id
    
    def get(self, id):
        return self.objects.get(id)
    
    def remove(self, id):
        del self.objects[id]

class Connection(object):
    
    _thread_local = local()
    
    def __init__(self, whither, loop=None, *, root=LocalRoot,
                 lformat='json', rformat='json', rformat_bin='msgpack',
                 logger=None, registry=None):
        self.root_factory = root
        self.loop = loop or get_event_loop()
        self.logger = logger or getLogger('eider')
        self.lencode = Codec.registry[lformat].encode
        self.rcodec = Codec.registry[rformat]
        self.rcodec_bin = Codec.registry.get(rformat_bin)
        
        # connection state
        self.lsessions = {}  # local sessions
        self.lcalls = {}  # local calls
        self.rcalls = {}  # remote calls
        self.bcalls = defaultdict(set)  # bridged calls
        self.nextrsid = 0  # next remote session id
        self.nextrcid = 0  # next remote call id
        
        LocalSession(self, None, LocalSessionManager)  # root session
        NativeLocalSession(self, -1, LocalRoot)  # native (non-LocalObject) session
        
        if registry is None:
            registry = getattr(self._thread_local, 'registry', None)
            if registry is None:
                self._thread_local.registry = registry = Registry()
        self.registry = registry
        self.id = registry.add(self)
        
        self.ws = None
        self.messages = []
        self.opened = Future(loop=self.loop)
        self.task = self.loop.create_task(self.receive(whither))
        self.closed = False
    
    @coroutine
    def wait_opened(self):
        opened = yield from self.opened
        if self.closed:
            raise DisconnectedError('Connection closed' if opened else 'Could not connect')
    
    def create_local_session(self, lsid=-2, root_factory=None, lformat=None):
        return LocalSession(self, lsid, root_factory, lformat)
    
    def create_session(self, lformat=None, rformat=None):
        return RemoteSession(self, lformat, rformat)
    
    def close(self):
        if self.closed:
            return
        self.closed = True
        self.task.cancel()
    
    @coroutine
    def wait_closed(self):
        yield from self.task
    
    @coroutine
    def __aenter__(self):
        return self
    
    @coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        yield from self.task
    
    @coroutine
    def receive(self, whither):
        http_session = None
        close_code = WSCloseCode.OK
        try:
            if isinstance(whither, str):
                http_session = ClientSession(loop=self.loop)
                self.ws = yield from http_session.ws_connect(whither)
            else:
                self.ws = whither
            
            self.opened.set_result(True)
            
            for header, body in self.messages:
                self.senddata(header)
                if body is not None:
                    self.senddata(body)
            self.messages = []
            
            header = None
            while 1:
                tp, data, extra = yield from self.ws.receive()
                
                if tp in (WSMsgType.TEXT, WSMsgType.BINARY):
                    if header is None:
                        rcodec = self.rcodec if tp == WSMsgType.TEXT else self.rcodec_bin
                        try:
                            msg = rcodec.decode(data)
                        except Exception as exc:
                            close_code = WSCloseCode.POLICY_VIOLATION
                            raise ProtocolError(
                                'Invalid data received on Eider WebSocket connection: {}: {}'
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
                    close_code = getattr(data, 'code', WSCloseCode.INTERNAL_ERROR)
                    raise ProtocolError('Eider WebSocket error: {}: {}'.format(
                        type(data).__name__, data))
                else:
                    close_code = WSCloseCode.UNSUPPORTED_DATA
                    raise ProtocolError(
                        'Received invalid Eider WebSocket message type {}'.format(tp))
            
        except CancelledError:
            close_code = WSCloseCode.GOING_AWAY
            # If we got here because close() was called, we should swallow the exception to avoid
            # cancelling an outer task.  If close() was not called, then we got here because an
            # outer task was cancelled, and we should not swallow it.
            if not self.closed:
                raise
        except ProtocolError:
            raise
        except Exception:
            close_code = WSCloseCode.INTERNAL_ERROR
            raise
        finally:
            if not self.opened.done():
                self.opened.set_result(False)
            self.closed = True
            self.lclose()
            self.rclose()
            if self.ws is not None:
                yield from self.ws.close(code=close_code)
            if http_session is not None:
                http_session.close()
    
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
                        raise AttributeError("Cannot call private method '{}'".format(method))
                    
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
                        # A subclass might override apply_begin() to execute asynchronously.
                        if cid is not None:
                            # mark the call as received but not yet applied
                            self.lcalls[cid] = False
                        result.add_done_callback(partial(self.on_apply_begin_done, srcid, cid))
                    else:
                        lsession, loid = result
                        lcodec = lsession.lcodec
                        try:
                            result = self.apply_finish(rcodec, srcid, method, lsession, loid, msg)
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
                            
                            result = self.getresult(rcodec, rcall.rsession, msg)
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
            header['src'] = self.id  # the callee needs to know where to send the response
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
        
        lsession = self.lsessions.get(lsid)
        if lsession is None:
            raise LookupError('Unknown session: {}'.format(lsid))
        
        return lsession, loid
    
    def apply_finish(self, rcodec, srcid, method, lsession, loid, msg):
        lobj = lsession.unmarshal_id(loid)
        f = getattr(lobj, method)
        params = lsession.unmarshal_all(rcodec, msg.get('params', []), srcid)
        return f(*params)
    
    def getresult(self, rcodec, rsession, msg):
        if 'result' in msg:
            return rsession.unmarshal_all(rcodec, msg['result'])
        else:
            if 'error' in msg:
                # attempt to unmarshal error object; fallback to generic Exception
                error = msg['error']
                message = str(error.get('message', '')) or 'Unspecified error'
                name = str(error.get('name'))
                etype = globals().get(name) or getattr(builtins, name, None)
                if not (isinstance(etype, type) and issubclass(etype, Exception)):
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
            fut.add_done_callback(partial(self.on_apply_finish_done, srcid, lcid, lcodec))
    
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
            result.add_done_callback(partial(self.on_done, srcid, lcid, lcodec))
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
                    src.error(None, cid, DisconnectedError('Bridged connection lost'))
        
        # dispose of outstanding bridged calls (as caller)
        for conn in self.registry.objects.values():
            cids = conn.bcalls.pop(self.id, [])
            if not conn.closed:
                for cid in cids:
                    conn.send({'cancel': cid})
    
    def sendcall(self, lcodec, dstid, rcid, robj, method, params=[]):
        if self.closed:
            raise DisconnectedError('Connection closed')
        
        header = {'dst': dstid,
                  'id': rcid,
                  'method': method}
        if lcodec is None:
            body = None
            header['this'] = robj
            header['params'] = params
        else:
            body = lcodec.encode(self,
                                 {'this': robj,
                                  'params': params})
            header['format'] = lcodec.name
        self.send(header, body)
    
    def respond(self, srcid, lcid, result, lcodec):
        header = {'dst': srcid, 'id': lcid}
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
        header = {'dst': srcid, 'id': lcid}
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
            except Exception as exc:
                body = None
                header['error'] = error
            else:
                header['format'] = lcodec.name
        self.send(header, body)
    
    def send(self, header, body=None):
        if not self.closed:
            header = self.lencode(self, header)
            if self.ws is None:
                self.messages.append((header, body))
                return
            
            self.senddata(header)
            if body is not None:
                self.senddata(body)
    
    def senddata(self, data):
        if isinstance(data, str):
            self.ws.send_str(data)
        else:
            self.ws.send_bytes(data)

class PeriodicCall:
    """Call a function periodically in an event loop.  This is preferable to using a Task because
    it doesn't force client code to call run_until_complete() after cancelling."""
    
    def __init__(self, f, interval, loop=None):
        self.f = f
        self.interval = interval
        self.loop = loop or get_event_loop()
        self.handle = self.loop.call_later(interval, self.run)
    
    def run(self):
        self.f()
        self.handle = self.loop.call_later(self.interval, self.run)
    
    def cancel(self):
        self.handle.cancel()

class BlockingMethod(RemoteMethod):
    
    __slots__ = ()
    
    def __call__(self, *params):
        return self.robj._rsession.conn.loop.run_until_complete(super().__call__(*params))

class BlockingObject(RemoteObject):
    
    __slots__ = ()
    
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
        """syntactic sugar: 'iter(obj)' --> 'obj.iter()' (with fallback to sequence protocol)"""
        try:
            return self.iter()
        except AttributeError:
            return SequenceIterator(self)
    
    def __next__(self):
        """syntactic sugar: 'next(obj)' --> 'obj.next()'"""
        # JavaScript-style iteration protocol: use a 'done' flag instead of StopIteration
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

class BlockingSession(BlockingSessionMixin, RemoteSession):
    
    __slots__ = ()
    
    def _open(self, rformat):
        # Efficiency is less of a concern here than in the asynchronous RemoteSession, so we raise
        # immediately if the session cannot be opened.
        self.conn.loop.run_until_complete(super()._open(rformat))

class BlockingBridgedSession(BlockingSessionMixin, BridgedSession):
    
    __slots__ = ()

class BlockingConnection:
    
    def __init__(self, url='ws://localhost:8080/', loop=None, **kwargs):
        if loop is None:
            loop = get_event_loop()
        
        # Allow SIGINT (Ctrl+C) on Windows.  Supposedly this is fixed in Python 3.5.  See:
        #   http://stackoverflow.com/questions/27480967/why-does-the-asyncios-event-loop-suppress-the-keyboardinterrupt-on-windows
        #   http://stackoverflow.com/questions/24774980/why-cant-i-catch-sigint-when-asyncio-event-loop-is-running
        self.busywait = PeriodicCall(lambda: None, 1, loop)
        
        self.conn = Connection(url, loop, **kwargs)
    
    def close(self):
        try:
            self.conn.close()
            self.conn.loop.run_until_complete(self.conn.wait_closed())
        except CancelledError:
            # This can happen during garbage collection at process exit.
            pass
        except ProtocolError as exc:
            self.conn.logger.warning(exc.msg)
        except Exception:
            self.conn.logger.exception('Error while closing connection')
        finally:
            self.busywait.cancel()
    
    # If this object is abandoned, make sure to close the underlying asynchronous Connection;
    # otherwise the Connection.receive() task will stay active.
    __del__ = close
    
    def create_local_session(self, *args, **kwargs):
        return self.conn.create_local_session(*args, **kwargs)
    
    def create_session(self, lformat=None, rformat=None):
        return BlockingSession(self.conn, lformat, rformat)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

@coroutine
def receive(url='ws://localhost:8080/', loop=None, **kwargs):
    conn = Connection(url, loop, **kwargs)
    yield from conn.wait_closed()

def serve(port=8080, loop=None, **kwargs):
    if loop is None:
        loop = get_event_loop()
    
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
    
    def on_shutdown(app):
        for conn in conns:
            conn.close()
    
    aiohttp2 = int(aiohttp_version[0]) >= 2
    
    app = Application(**({} if aiohttp2 else {'loop': loop}))
    app.router.add_route('GET', '/', handle)
    app.on_shutdown.append(on_shutdown)
    
    busywait = PeriodicCall(lambda: None, 1, loop)  # see comment for BlockingConnection.busywait
    try:
        run_app(app, port=port, **({'loop': loop} if aiohttp2 else {}))
    finally:
        busywait.cancel()

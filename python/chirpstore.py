#!/usr/bin/env python3
#
# A minimalistic client for the chirpstore RPC service.
#
# WARNING: Not complete.
#
import json, socket, struct

class Client(object):
    """
    A minimalistic non-concurrent client for the chirpstore service.

    Exceptions thrown from calls to the service have type ServiceError.
    """
    PKT_REQUEST  = 2
    PKT_RESPONSE = 4

    M_STATUS  = b'status'
    M_GET     = b'get'
    M_PUT     = b'put'
    M_DELETE  = b'delete'
    M_LIST    = b'list'
    M_LEN     = b'len'
    M_CAS_PUT = b'cas-put'
    M_CAS_KEY = b'cas-key'

    ERR_KEY_EXISTS    = 400
    ERR_KEY_NOT_FOUND = 404

    def __init__(self, socket):
        """Initialize a new client with a connected socket.
        """
        self._conn = Conn(socket)
        self._reqid = 0

    def status(self):
        """Report server status.
        """
        return json.loads(self.__call(self.M_STATUS))

    def len(self):
        """Report the number of keys in the store.
        """
        return struct.unpack('>Q', self.__call(self.M_LEN))[0]

    def list(self, count=0, start=b''):
        """List up to count keys in the store beginning at or after the given
        starting key in lexicographic order.
        """
        return ListResponse(self.__call(self.M_LIST, ListRequest(count, start).payload))

    def get(self, key):
        """Fetch the data associated with the given key, or raise KeyError.
        """
        try:
            return self.__call(self.M_GET, key)
        except ServiceError as e:
            if e.code == self.ERR_KEY_NOT_FOUND:
                raise KeyError(key) from e
            raise

    def size(self, key):
        """Fetch the size of the specified key, or raise KeyError.
        """
        try:
            data = self.__call(self.M_SIZE, key)
            return struct.unpack('>Q', data)[0]
        except ServiceError as e:
            if e.code == self.ERR_KEY_NOT_FOUND:
                raise KeyError(key) from e
            raise

    # TODO: put, delete, casput, caskey

    def __read_packet(self):
        hdr = self._conn.read(8)
        sig, ptype, plen = struct.unpack('>3sbI', hdr)
        if sig != b'CP\x00':
            raise ProtocolError("invalid packet header")

        payload = self._conn.read(plen)
        if len(payload) != plen:
            raise ProtocolError("packet payload truncated")
        return ptype, payload

    def __call(self, method_id, payload=b''):
        req_id, request = self.__request(method_id, payload)
        self._conn.write(self.__packet(self.PKT_REQUEST, request))

        ptype, result = self.__read_packet()
        if ptype != self.PKT_RESPONSE:
            raise ProtocolError(f'unexpected packet type {ptype}')

        rsp_id, code = struct.unpack('>Ib', result[:5])
        if rsp_id != req_id:
            raise ProtocolError(f'unexpected response id, got {rsp_id}, want {req_id}')
        elif code != 0:
            raise ServiceError(code, result[5:])
        return result[5:]

    fmt_req = struct.Struct('>IB')

    def __request(self, method_id, payload=b''):
        self._reqid += 1
        return self._reqid, self.fmt_req.pack(self._reqid, len(method_id))+method_id+payload

    fmt_pkt = struct.Struct('>3sbI')

    def __packet(self, ptype, payload):
        return self.fmt_pkt.pack(b'CP\x00', ptype, len(payload))+payload

    def __del__(self):
        self._conn.close()


class ServiceError(Exception):
    fmt = struct.Struct('>H')

    def __init__(self, etype, data):
        self.etype = etype
        self.payload = data
        self.code, self.message, self.aux = 0, b'', b''

        if len(data) != 0:
            self.code = self.fmt.unpack(data[:self.fmt.size])[0]
            self.message, self.aux = b'', b''

            if len(data) > self.fmt.size:
                end = self.fmt.size*2
                n = self.fmt.unpack(data[self.fmt.size:end])[0]
                self.message = data[:end+n]
                self.aux = data[end+n:]


class ProtocolError(Exception):
    pass

class Conn(object):
    """A wrapper around a socket.socket that provides read and write methods.
    """
    def __init__(self, s):
        self._socket = s

    def read(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self._socket.recv(n-len(buf))
            if len(chunk) == 0:
                break
            buf.extend(chunk)
        return bytes(buf)

    def write(self, data):
        pos = 0
        while pos < len(data):
            nw = self._socket.send(data[pos:])
            if nw == 0:
                raise RuntimeError("socket connection closed")
            pos += nw

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

class PutRequest(object):
    fmt = struct.Struct('>bH')

    def __init__(self, key, data, replace=False):
        self.payload = self.fmt.Pack(int(replace), len(key)) + key + data

class ListRequest(object):
    fmt = struct.Struct('>I')

    def __init__(self, count, start=b''):
        self.payload = self.fmt.pack(count) + start

class ListResponse(object):
    fmt = struct.Struct('>H')

    def __init__(self, data):
        self.payload = data
        self.keys = []
        self.next = None

        i = 0
        while i < len(data):
            size = self.fmt.unpack(data[i:i+self.fmt.size])[0]
            i += self.fmt.size
            if self.next is None:
                self.next = Key(data[i:i+size])
            else:
                self.keys.append(Key(data[i:i+size]))
            i += size

    def has_more(self):
        return bool(self.next)


class Key(bytes):
    def __repr__(self):
        return super().hex()

def dial(addr):
    """Connect a TCP socket to the given address:port.
    """
    host, port = addr.split(':', 1)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    s.connect((host, int(port)))
    return s

__export__ = ('Client', 'dial')

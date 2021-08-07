import time
import logging

import msgpack

import zmq


logger = logging.getLogger(__name__)

class RPCError(Exception):
    """
    Exception that shows bad or broken network.
    When raised from zmqrpc, RPC should be reestablished.
    """
    pass


def retry(delays=(1, 3, 5), exception=Exception):
    def decorator(function):
        def wrapper(*args, **kwargs):
            cnt = 0
            for delay in delays + (None,):
                try:
                    return function(*args, **kwargs)
                except exception as e:
                    if delay is None:
                        logger.info("Retry failed after %d times." % (cnt))
                        raise
                    else:
                        cnt += 1
                        logger.info("Exception found on retry %d: -- retry after %ds" % (cnt, delay))
                        logger.exception(e)
                        time.sleep(delay)

        return wrapper

    return decorator


class Message:
    def __init__(self, message_type, data, node_id):
        self.type = message_type
        self.data = data
        self.node_id = node_id

    def __repr__(self):
        return "<Message %s:%s>" % (self.type, self.node_id)

    def serialize(self):
        return msgpack.dumps((self.type, self.data, self.node_id))

    @classmethod
    def unserialize(cls, data):
        msg = cls(*msgpack.loads(data, raw=False, strict_map_key=False))
        return msg


class BaseSocket:
    def __init__(self, sock_type):
        context = zmq.Context()
        self.socket = context.socket(sock_type)

        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)

    @retry()
    def send(self, msg):
        try:
            self.socket.send(msg.serialize(), zmq.NOBLOCK)
        except zmq.error.ZMQError as e:
            raise RPCError("ZMQ sent failure") from e

    @retry()
    def send_to_client(self, msg):
        try:
            self.socket.send_multipart([msg.node_id.encode(), msg.serialize()])
        except zmq.error.ZMQError as e:
            raise RPCError("ZMQ sent failure") from e

    def recv(self):
        try:
            data = self.socket.recv()
            msg = Message.unserialize(data)
        except msgpack.exceptions.ExtraData as e:
            raise RPCError("ZMQ interrupted message") from e
        except zmq.error.ZMQError as e:
            raise RPCError("ZMQ network broken") from e
        return msg

    def recv_from_client(self):
        try:
            data = self.socket.recv_multipart()
            addr = data[0].decode()
            msg = Message.unserialize(data[1])
        except (UnicodeDecodeError, msgpack.exceptions.ExtraData) as e:
            raise RPCError("ZMQ interrupted message") from e
        except zmq.error.ZMQError as e:
            raise RPCError("ZMQ network broken") from e
        return addr, msg

    def close(self):
        self.socket.close()


class Server(BaseSocket):
    def __init__(self, host, port):
        BaseSocket.__init__(self, zmq.ROUTER)
        if port == 0:
            self.port = self.socket.bind_to_random_port("tcp://%s" % host)
        else:
            try:
                self.socket.bind("tcp://%s:%i" % (host, port))
                self.port = port
            except zmq.error.ZMQError as e:
                raise RPCError("Socket bind failure: %s" % (e))


class Client(BaseSocket):
    def __init__(self, host, port, identity):
        BaseSocket.__init__(self, zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, identity.encode())
        self.socket.connect("tcp://%s:%i" % (host, port))

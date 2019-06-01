import cbor2
import time
import logging
import zmq

import uuid

# If no server replies within this time, abandon request
GLOBAL_TIMEOUT = 4000    # msecs
# PING interval for servers we think are alivecp
PING_INTERVAL = 1000    # msecs
# Server considered dead if silent for this long
SERVER_TTL = 12000    # msecs


class FreelanceClient(object):
    ctx = None      # Our Context
    router = None  # Socket to talk to servers
    server = None  # Server we've connected to
    request = None  # Current request if any
    reply = None  # Current reply if any
    expires = 0  # Timeout for request/reply

    def __init__(self, optional_global_timeout=4000):
        self.ctx = zmq.Context.instance()
        self.global_timeout = optional_global_timeout
        self.router = self.ctx.socket(zmq.DEALER)
        self.router.setsockopt_unicode(zmq.IDENTITY, str(uuid.uuid4()))
        # self.router.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)

    def connect(self, endpoint):
        logging.debug("I: connecting to " + endpoint)
        self.router.connect(endpoint)
        self.server = endpoint
        time.sleep(0.3)

    def stop(self):
        logging.debug('got the idea to stop, closing the socket')
        self.poller.unregister(self.router)
        self.router.disconnect(self.server)
        self.router.close()

    def send_and_receive(self, msg):
        try:
            assert self.request is None
            self.request = [self.server.encode('utf8')] + [msg]
            self.router.send_multipart(self.request)
            events = dict(self.poller.poll(timeout=self.global_timeout))
            if self.router in events:
                reply = self.router.recv_multipart()
                self.request = None
                return reply[-1]
        except Exception as e:
            logging.exception(e)
            raise e


class ZMQFLPClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str, total_timeout=4000):
        self.clients = []
        for ip in list_of_server_ips_with_ports_as_str:
            this_client = FreelanceClient(optional_global_timeout=total_timeout)
            logging.info('client: connecting to server '+ip)
            this_client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)
            self.clients.append(this_client)

    def __str__(self):
        return str([str(x.server) for x in self.clients])

    def send_and_receive(self, in_request):
        the_client = self.clients.pop(0)
        reply = the_client.send_and_receive(cbor2.dumps(in_request))  # , use_bin_type=True))
        self.clients.append(the_client)
        if reply:
            return cbor2.loads(reply)  # , raw=False, encoding="utf-8")
        else:
            raise ValueError("error, request " + str(in_request) + " unserviced")

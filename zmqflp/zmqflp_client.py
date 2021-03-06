import cbor2
import time
import logging
import zmq

import uuid


class FreelanceClient(object):
    ctx = None      # Our Context
    router = None  # Socket to talk to servers
    server = None  # Server we've connected to
    request = None  # Current request if any
    reply = None  # Current reply if any
    expires = 0  # Timeout for request/reply

    def __init__(self, global_timeout):
        self.ctx = zmq.Context.instance()
        self.global_timeout = global_timeout
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
            self.request = None
            if self.router in events:
                reply = self.router.recv_multipart()
                return cbor2.loads(reply[-1])
            else:
                return None
        except Exception as e:
            logging.exception(e)
            raise e


class ZMQFLPClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str, total_timeout=7000):
        self.clients = []
        for ip in list_of_server_ips_with_ports_as_str:
            this_client = FreelanceClient(global_timeout=total_timeout)
            logging.info('client: connecting to server '+ip)
            this_client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)
            self.clients.append(this_client)

    def __str__(self):
        return str([str(x.server) for x in self.clients])

    def send_and_receive(self, in_request):
        incoming_request = cbor2.dumps(in_request)
        for the_client in self.clients:
            reply = the_client.send_and_receive(incoming_request)
            if reply is not None:
                return reply
        raise ValueError("error, request " + str(in_request) + " unserviced")

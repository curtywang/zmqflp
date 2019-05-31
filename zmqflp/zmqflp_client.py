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
    servers = None  # Servers we've connected to
    actives = None  # Servers we know are alive
    sequence = 0  # Number of requests ever sent
    request = None  # Current request if any
    reply = None  # Current reply if any
    expires = 0  # Timeout for request/reply

    def __init__(self, optional_global_timeout=4000):
        self.ctx = zmq.Context()
        self.global_timeout = optional_global_timeout
        self.router = self.ctx.socket(zmq.ROUTER)
        self.router.setsockopt_unicode(zmq.IDENTITY, str(uuid.uuid4()))
        self.router.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self.actives = []

    def connect(self, endpoint):
        logging.debug("I: connecting to " + endpoint)
        self.router.connect(endpoint)
        self.actives.append(endpoint)
        time.sleep(0.3)

    def stop(self):
        logging.debug('got the idea to stop, closing the socket')
        for server in self.actives:
            self.router.disconnect(server)
        self.router.close()
        # logging.info('terminating context')
        # self.ctx.term()

    def send_and_receive(self, msg):
        assert not self.request
        endpoint = self.actives.pop(0)
        self.request = [endpoint.encode('utf8')] + [str(self.sequence).encode('utf8')] + [msg]
        self.expires = time.time() + 1e-3 * self.global_timeout
        self.router.send_multipart(self.request)
        # TODO: this should be a while loop?
        reply = self.router.recv_multipart()
        # Frame 0 is server that replied
        the_server = reply[0].decode('utf8')
        self.actives.append(the_server)
        if reply[1].decode('utf8') != 'PONG':
            # Frame 1 may be sequence number for reply
            sequence = reply[1].decode('utf8')
            if int(sequence) == self.sequence:
                self.sequence += 1
                self.request = None
                return reply[2]
        else:
            return False


class ZMQFLPClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str, total_timeout=4000):
        self.client = FreelanceClient(optional_global_timeout=total_timeout)
        for ip in list_of_server_ips_with_ports_as_str:
            logging.info('client: connecting to server '+ip)
            self.client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)

    def __str__(self):
        return str(self.client.servers)

    def send_and_receive(self, in_request):
        reply = self.client.send_and_receive(cbor2.dumps(in_request))  # , use_bin_type=True))
        if not reply:
            raise ValueError("error, request "+str(in_request)+" unserviced")
        else:
            return cbor2.loads(reply)  # , raw=False, encoding="utf-8")

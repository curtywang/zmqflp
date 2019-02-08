"""
flcliapi - Freelance Pattern agent class
Model 3: uses ROUTER socket to address specific services
Author: Min RK <benjaminrk@gmail.com>
Modified by: Curtis Wang <ycwang@u.northwestern.edu>

LRU queue is used to connect to servers to add load balancing
Also allows for a context-manager-based client
"""

import threading
import time
import logging
import os
import asyncio
import binascii
import zmq

# If no server replies within this time, abandon request
GLOBAL_TIMEOUT = 4000    # msecs
# PING interval for servers we think are alivecp
PING_INTERVAL  = 1000    # msecs
# Server considered dead if silent for this long
SERVER_TTL     = 12000    # msecs


# =====================================================================
# Synchronous part, works in our application thread

class FreelanceClient(object):
    ctx = None      # Our Context
    pipe = None     # Pipe through to flciapi agent
    agent = None    # agent in a thread

    def __init__(self, optional_global_timeout=4000):
        self.ctx = zmq.asyncio.Context()
        self.global_timeout = optional_global_timeout
        self.pipe, self.peer = self.zpipe(self.ctx)
        self.threadevent = threading.Event()
        self.threadevent.set()
        #self.agent = threading.Thread(target=agent_task, args=(self.ctx, self.peer, self.threadevent, self.global_timeout))
        #self.agent.daemon = True
        #self.agent.start()

    def zpipe(self, ctx):
        """build inproc pipe for talking to threads
        mimic pipe used in czmq zthread_fork.
        Returns a pair of PAIRs connected via inproc
        """
        a = ctx.socket(zmq.PAIR)
        b = ctx.socket(zmq.PAIR)
        a.linger = b.linger = 0
        a.hwm = b.hwm = 1
        #iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
        iface = 'tcp://127.0.0.1'
        port = a.bind_to_random_port(iface, min_port=10000, max_port=65536, max_tries=10)
        b.connect(iface+':'+str(port))
        time.sleep(0.5)
        return a,b

    def connect(self, endpoint):
        """Connect to new server endpoint
        Sends [CONNECT][endpoint] to the agent
        """
        self.agent = asyncio.run_coroutine_threadsafe(agent_task(self.ctx, self.peer, self.threadevent, self.global_timeout), asyncio.get_running_loop())
        time.sleep(0.1) # Allow connection to come up
        self.pipe.send_multipart(["CONNECT".encode('utf8'), endpoint.encode('utf8')])
        time.sleep(0.1) # Allow connection to come up

    async def request(self, msg):
        request = ["REQUEST".encode('utf8'), msg]#.encode('utf8')]
        await self.pipe.send_multipart(request)
        logging.debug('>> waiting for reply from agent... ')
        reply = await self.pipe.recv_multipart()
        logging.debug('>> reply was: '+str(reply))
        status = reply.pop(0).decode('utf8')
        if status != "FAILED":
            return reply
        else:
            return None

    def stop(self):
        print("stopping agent thread...")
        self.threadevent.clear()
        self.agent.join(timeout=0.5)
        print("terminating agent context...")
        self.pipe.close()
        self.peer.close()
        self.ctx.term()
        self.ctx.destroy()


# =====================================================================
# Asynchronous part, works in the background

# ---------------------------------------------------------------------
# Simple class for one server we talk to

class FreelanceServer(object):
    endpoint = None         # Server identity/endpoint
    alive = True            # 1 if known to be alive
    ping_at = 0             # Next ping at this time
    expires = 0             # Expires at this time

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.alive = True
        self.ping_at = time.time() + 1e-3*PING_INTERVAL
        self.expires = time.time() + 1e-3*SERVER_TTL

    def ping(self, socket):
        if time.time() > self.ping_at:
            socket.send_multipart([self.endpoint, 'PING'.encode('utf8')])
            self.ping_at = time.time() + 1e-3*PING_INTERVAL

    def tickless(self, tickless):
        if tickless > self.ping_at:
            tickless = self.ping_at
        return tickless

# ---------------------------------------------------------------------
# Simple class for one background agent

class FreelanceAgent(object):
    ctx = None              # Own context
    pipe = None             # Socket to talk back to application
    router = None           # Socket to talk to servers
    servers = None          # Servers we've connected to
    actives = None          # Servers we know are alive
    sequence = 0            # Number of requests ever sent
    request = None          # Current request if any
    reply = None            # Current reply if any
    expires = 0             # Timeout for request/reply

    def __init__(self, ctx, pipe, global_timeout):
        self.ctx = ctx
        self.pipe = pipe
        self.router = ctx.socket(zmq.ROUTER)
        self.servers = {}
        self.actives = []
        self.global_timeout = global_timeout

    def control_message (self):
        msg = self.pipe.recv_multipart()
        command = msg.pop(0).decode('utf8')

        if command == "CONNECT":
            endpoint = msg.pop(0).decode('utf8')
            logging.info("I: connecting to %s..." % endpoint)
            self.router.connect(endpoint.encode('utf8'))
            server = FreelanceServer(endpoint.encode('utf8'))
            self.servers[endpoint.encode('utf8')] = server
            self.actives.append(server)
            # these are in the C case, but seem redundant:
            server.ping_at = time.time() + 1e-3*PING_INTERVAL
            server.expires = time.time() + 1e-3*SERVER_TTL
        elif command == "REQUEST":
            assert not self.request    # Strict request-reply cycle
            # Prefix request with sequence number and empty envelope
            self.request = [str(self.sequence).encode('utf8')] + msg
            # Request expires after global timeout
            self.expires = time.time() + 1e-3*self.global_timeout

    def router_message (self):
        reply = self.router.recv_multipart()
        #logging.info('zmqreply: '+str(reply))
        # Frame 0 is server that replied
        endpoint = reply[0]
        server = self.servers[endpoint]
        if not server.alive: # re-add if it comes back alive
            self.actives.append(server)
            server.alive = 1

        server.ping_at = time.time() + 1e-3*PING_INTERVAL
        server.expires = time.time() + 1e-3*SERVER_TTL

        if reply[1].decode('utf8') != 'PONG':
            # Frame 1 may be sequence number for reply
            sequence = reply[1].decode('utf8')
            if int(sequence) == self.sequence:
                self.sequence += 1
                reply = ["OK".encode('utf8')] + reply
                self.pipe.send_multipart(reply)
                self.request = None


# ---------------------------------------------------------------------
# Asynchronous agent manages server pool and handles request/reply
# dialog when the application asks for it.

async def agent_task(ctx, pipe, threadevent, global_timeout):
    agent = FreelanceAgent(ctx, pipe, global_timeout)
    logging.info('registering client agent...')
    poller = zmq.asyncio.Poller()
    poller.register(agent.pipe, zmq.POLLIN)
    poller.register(agent.router, zmq.POLLIN)
    logging.info('done registering client agent!')

    while threadevent.is_set():
        # Calculate tickless timer, up to 1 hour
        tickless = time.time() + 120
        if (agent.request and tickless > agent.expires):
            tickless = agent.expires
            for server in agent.servers.values():
                tickless = server.tickless(tickless)
        try:
            items = dict(poller.poll(1000 * (tickless - time.time())))
        except (zmq.ContextTerminated, zmq.error.ZMQError) as e:
            return False              # Context has been shut down

        if agent.pipe in items:
            agent.control_message()

        if agent.router in items:
            agent.router_message()

        # If we're processing a request, dispatch to next server
        if (agent.request):
            if (time.time() >= agent.expires):
                # Request expired, kill it
                agent.pipe.send("FAILED".encode('utf8'))
                agent.request = None
            else:
                # Find server to talk to, remove any expired ones
                while agent.actives:
                    server = agent.actives.pop(0)
                    if time.time() >= server.expires:
                        server.alive = 0
                    else:
                        request = [server.endpoint] + agent.request
                        agent.router.send_multipart(request)
                        if server not in agent.actives:
                            agent.actives.append(server) # maintian LRU queue
                        break

        # Disconnect and delete any expired servers
        # Send heartbeats to idle servers if needed
        for server in agent.servers.values():
            server.ping(agent.router)
import asyncio
import sys
import zmq
import zmq.asyncio
import socket
import msgpack
import logging

class ZMQFLPServer(object):
    def __init__(self, custom_identity = None, str_port = '9001'):
        ctx = zmq.asyncio.Context()
        # Prepare server socket with predictable identity
        if custom_identity:
            identity = custom_identity
        else:
            identity = socket.gethostbyname(socket.gethostname())
        bind_endpoint = "tcp://*:"+str(str_port)
        connect_endpoint = ('tcp://'+identity+':'+str(str_port)).encode('utf8')
        self.server = ctx.socket(zmq.ROUTER)
        self.server.setsockopt(zmq.IDENTITY, connect_endpoint)
        self.server.bind(bind_endpoint)
        logging.info("I: service is ready with identity " + str(connect_endpoint))
        logging.info("I: service is bound to " + str(bind_endpoint))

        self.message_table = {}


    async def receive(self):
        # Frame 0: identity of client
        # Frame 1: PING, or client control frame
        # Frame 2: request body
        try:
            request = await self.server.recv_multipart()
            control = request[1].decode('utf8')
        except asyncio.CancelledError:
            return (None, None) # Interrupted
        except Exception as e:
            logging.exception(e)
            return (None, None) # Interrupted
        if control == "PING":
            await self.send([request[0]], "PONG".encode('utf8'), mpack=False)
            return (request[1].decode('utf8'), [request[0]])
        else:
            if request[0] not in self.message_table:
                self.message_table[request[0]] = request[1]
                return (msgpack.loads(request[2], raw=False, encoding="utf-8"), request[0:2])
            elif self.message_table[request[0]] == request[1]:
                return (None, None)
            else:
                self.message_table[request[0]] = request[1]
                return (msgpack.loads(request[2], raw=False, encoding="utf-8"), request[0:2])


    async def send(self, orig_req_headers, str_resp, mpack=True):
        out_message = orig_req_headers
        if mpack:
            out_message.append(msgpack.dumps(str_resp, use_bin_type=True))#.encode('utf8'))
        else:
            out_message.append(str_resp)
        await self.server.send_multipart(out_message)
# zmqflp

Improvements to the Freelance protocol-based zeromq server/client (Python)
The client and server talk using msgpack, so the api accepts dictionaries as input.

## To create a zmqflp server:

```python
# create the server object (it runs in an asyncio zmq context)
self.server = zmqflp_server.ZMQFLPServer(self.config.identity, self.config.zmq_port)

# use the following code to process messages received by the server and send them back
async def process_messages(self):
    (serialized_request, orig_headers) = await self.server.receive()
    if serialized_request == 'EXIT':
        await self.server.send(orig_headers, 'exiting')
        return False
    elif serialized_request != "PING":
        try:
            request = serialized_request
            response = self.process_request(request)
            await self.server.send(orig_headers, response)
            return True
        except Exception as e:
            logging.exception(e)
            return False
    return True
```

## To create a client without using a context manager:

```python
# create the client object (this does NOT run in an asyncio context)
self.client = zmqflp_client.ZMQFLPClient(self.config.list_of_servers)

# to send and receive with the client
msg_to_send = {'message': 'hello!', 'other-key': 'stuff goes here'}
status = self.client.send_and_receive(msg_to_send)
```

## To create a client using a context manager (for example, to run on AWS Lambda):

```python
# create the client object (this does NOT run in an asyncio context)
with zmqflp_client.ZMQFLPClient(self.config.list_of_servers) as client:
    # to send and receive with the client
    msg_to_send = {'message': 'hello!', 'other-key': 'stuff goes here'}
    status = client.send_and_receive(msg_to_send)
```
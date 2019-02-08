import zmqflp_client
import zmqflp_server
import time
import multiprocessing
import socket
import asyncio
import logging
import msgpack


LEN_TEST_MESSAGE = 1000


def server_main():
    asyncio.get_event_loop().run_until_complete(asyncio.wait([
        server_loop()
    ]))
    return 0

async def server_loop():
    logger = logging.getLogger()
    server = zmqflp_server.ZMQFLPServer(str_port='9001')
    keep_running = True
    while keep_running:
        # handle the "TEST" requests
        (str_request, orig_headers) = await server.receive()
        req_object = msgpack.loads(str_request)
        if req_object != "EXIT":
            await server.send(orig_headers, req_object)
        elif req_object == "EXIT":
            logger.info('server exiting...')
            await server.send(orig_headers, "EXITING")
            keep_running = False
    if keep_running is False:
        return


async def run_test(client, num_of_tests):
    for i in range(num_of_tests):
        test_message = ["TEST" for i in range(LEN_TEST_MESSAGE)]
        reply = await client.send_and_receive(msgpack.dumps(test_message))
        #logging.debug('reply: '+str(reply))
        if len(reply) != LEN_TEST_MESSAGE:#"TEST_OK":
            logging.debug("TEST_FAILURE")
            raise ValueError()
    logging.debug("ending client send")
    client.send_and_receive(msgpack.dumps("EXIT"))
    return


def main():
    log_handlers = [logging.StreamHandler()]
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S', 
        handlers=log_handlers, 
        level=logging.DEBUG)
    requests = 1
    logging.debug(">> creating server process...")
    new_ctx = multiprocessing.get_context('spawn')
    server_process = new_ctx.Process(target=server_main, daemon=True)
    server_process.start()
    logging.debug(">> server process created!")
    time.sleep(0.5)
    #client_process = Process(target=client_main, args=(requests,))
    client = zmqflp_client.ZMQFLPClient([socket.gethostbyname(socket.gethostname())+':9001'])

    logging.debug(">> starting zmq freelance protocol test!")
    start = time.time()
    asyncio.run(run_test(client, requests))
    #client_process.start()
    #client_process.join()
    avg_time = ((time.time() - start) / requests)
    logging.debug(">> waiting for server to exit...")
    server_process.join()
    logging.debug("Average RT time (sec): "+str(avg_time))
    return


if __name__ == '__main__':
    main()
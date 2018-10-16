from zmqflp import zmqflp_client
from zmqflp import zmqflp_server
import time
from multiprocessing import Process
import socket

def server_main():
    server = zmqflp_server.ZMQFLPServer(str_port='9001')
    while True:
        # handle the "TEST" requests
        (str_request, orig_headers) = server.receive()
        if str_request == "TEST":
            server.send(orig_headers, "TEST_OK")
        elif str_request == "EXIT":
            server.send(orig_headers, "EXITING")
            return

def client_main(num_of_tests):
    client = zmqflp_client.ZMQFLPClient([socket.gethostbyname(socket.gethostname())+':9001'])
    for i in range(num_of_tests):
        reply = client.send_and_receive("TEST")
        if reply != "TEST_OK":
            print("TEST_FAILURE")
    client.send_and_receive("EXIT")
    return

def main():
    requests = 10000
    server_process = Process(target=server_main)
    server_process.start()
    time.sleep(0.5)
    client_process = Process(target=client_main, args=(requests,))

    print(">> starting zmq freelance protocol test!")
    start = time.time()
    client_process.start()
    client_process.join()
    avg_time = ((time.time() - start) / requests)
    server_process.join()
    print("Average RT time (sec): "+str(avg_time))
    return

if __name__ == '__main__':
    main()
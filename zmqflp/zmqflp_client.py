import time
import msgpack
import logging
import zmqflp_api

class ZMQFLPClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str, total_timeout=4000):
        self.client = zmqflp_api.FreelanceClient(optional_global_timeout=total_timeout)
        time.sleep(0.1)
        for ip in list_of_server_ips_with_ports_as_str:
            logging.info('client: connecting to server '+ip)
            self.client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)

    def send_and_receive(self, in_request):
        reply = self.client.request(msgpack.dumps(in_request))
        if not reply:
            logging.info("error, request "+str(in_request)+" unserviced")
            return False
        else:
            return msgpack.loads(reply[2], encoding="utf-8")
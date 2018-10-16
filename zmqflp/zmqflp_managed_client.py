import time
import umsgpack
import logging
from .zmqflp_api import FreelanceClient

class ZMQFLPLambdaClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str):
        self.client = FreelanceClient()
        time.sleep(0.1)
        for ip in list_of_server_ips_with_ports_as_str:
            logging.info('client: connecting to server '+ip)
            self.client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)

    def __enter__(self):
        return self

    # TODO: re-add clients once they die
    def send_and_receive(self, in_request):
        reply = self.client.request(umsgpack.dumps(in_request))
        if not reply:
            logging.info("error, request "+str(in_request)+" unserviced")
            return False
        else:
            return umsgpack.loads(reply[2])#.decode('utf8')
    
    def __exit__(self, *args):
        print('stopping client...')
        self.client.stop()
import cbor2
import logging
from .zmqflp_api import FreelanceClient
# from zmqflp_api import FreelanceClient

# Client usable with Context Managers
# needed for containerized python jobs


class ZMQFLPManagedClient(object):
    def __init__(self, list_of_server_ips_with_ports_as_str):
        self.client = FreelanceClient()
        for ip in list_of_server_ips_with_ports_as_str:
            logging.debug('client: connecting to server '+ip)
            self.client.connect("tcp://"+ip)
            logging.info('client: added server '+ip)

    def __enter__(self):
        return self

    def send_and_receive(self, in_request):
        reply = self.client.send_and_receive(cbor2.dumps(in_request))  # , use_bin_type=True))
        if not reply:
            raise ValueError('request unserviced!')
        else:
            return cbor2.loads(reply)  # , raw=False, encoding="utf-8")
    
    def __exit__(self, *args):
        try:
            logging.debug('stopping client...')
            self.client.stop()
        except Exception as e:
            return

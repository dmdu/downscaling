#!/usr/bin/env python

import logging
import signal
import time
from threading import Thread

from lib.logger import configure_logging
from lib.util import parse_options
from lib.config import Config
from resources.clouds import Clouds
from resources.master import Master
from resources.initialmonitor import InitialMonitor
from resources.policy import Policy
from resources.phantom import PhantomClient

SIGEXIT = False
LOG = logging.getLogger(__name__)

class Cleanup(Thread):
    def __init__(self, config):

        Thread.__init__(self)
        self.config = config

    def run(self):
        LOG.info("Starting Cleanup")

        phantom_client = PhantomClient(self.config, master=None)
        phantom_client.connect()
        phantom_client.delete_all_launch_config()
        phantom_client.delete_all_domains()

        all_domains = phantom_client.conn.get_all_groups()
        for domain in all_domains:
            print phantom_client.get_autoscale_groups_info(domain.name)

def main():
    (options, args) = parse_options()
    configure_logging(options.debug)
    config = Config(options)

    cleanup = Cleanup(config)
    cleanup.start()

if __name__ == "__main__":
    main()
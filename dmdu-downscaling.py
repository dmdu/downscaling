#!/usr/bin/env python

import logging
import signal
import time
from threading import Thread, Event

from lib.logger import configure_logging
from lib.util import parse_options
from lib.config import Config
from resources.clouds import Clouds
from resources.master import Master
from resources.workers_phantom import Workers
from resources.workload import Workload
from resources.initialmonitor import InitialMonitor
from resources.replacer import Replacer
from lib.util import Command
from resources.policy import Policy
from resources.phantom import PhantomClient

SIGEXIT = False
LOG = logging.getLogger(__name__)

class Downscaling(Thread):
    def __init__(self, config):

        Thread.__init__(self)
        self.config = config

    def run(self):
        LOG.info("Starting Downscaling")
        #TODO(dmdu): do something

        self.clouds = Clouds(self.config)

        self.phantom_client = PhantomClient(self.config)
        self.phantom_client.connect()
        self.phantom_client.create_launch_configs()
        self.phantom_client.create_auto_scaling_group(given_total_vms=0)

        self.phantom_client.asg.set_capacity(10)
        for i in range(5):
            self.phantom_client.print_info()
            self.clouds.log_instance_distribution()
            time.sleep(60)

        #self.phantom_client.suspend() # Giving errors, maybe not supported

        # pick 2 instance in sierra
        sierra = self.clouds.lookup_by_name("sierra")
        sierra_instances = sierra.get_instances()
        sierra_instance_names = []
        for instance in sierra_instances[:2]:
            sierra_instance_names.append(instance.id)
        LOG.info("Selected instances for termination: %s" % str(sierra_instance_names))

        for instance_name in sierra_instance_names:
            LOG.info("Terminating instance %s" % (instance_name))
            self.phantom_client.conn.terminate_instance(instance_name, decrement_capacity=True)

        self.phantom_client.update_tags("hotel:7,sierra:3", 5)
        #self.phantom_client.resume() # Haven't tested yet, commented out because suspend gives errors

        for i in range(10):
            self.phantom_client.print_info()
            self.clouds.log_instance_distribution()
            time.sleep(60)

        return

def clean_exit(signum, frame):
    global SIGEXIT
    SIGEXIT = True
    LOG.critical("Exit signal received. Exiting at the next sane time. "
                 "Please stand by.")

def main():
    (options, args) = parse_options()
    configure_logging(options.debug)

    config = Config(options)

    signal.signal(signal.SIGINT, clean_exit)
    downscaling = Downscaling(config)
    downscaling.start()

    # wake every seconed to make sure signals are handled by the main thread
    # need this due to a quirk in the way Python threading handles signals
    while downscaling.isAlive():
        downscaling.join(timeout=1.0)

if __name__ == "__main__":
    main()
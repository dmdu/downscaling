#!/usr/bin/env python

import logging
import signal
import time
from threading import Thread, Event
import os

from lib.logger import configure_logging
from lib.util import parse_options
from lib.util import read_config
from lib.config import Config
from resources.clouds import Clouds
from resources.master import Master
from resources.workers import Workers
from resources.workload import Workload
from resources.monitor import Monitor
from resources.failuresimulator import FailureSimulator, ExpFailureSimulator, ExpFailureSimulatorInOneCloud
from resources.initialmonitor import InitialMonitor
from resources.replacer import Replacer

SIGEXIT = False
LOG = logging.getLogger(__name__)


class Downscaling(Thread):
    def __init__(self, config):

        Thread.__init__(self)
        self.config = config

    def run(self):
        LOG.info("Starting Downscaling")
        #TODO(dmdu): do something

        # Get information about available clouds
        self.clouds = Clouds(self.config)

        # Potentially terminate some instances from the previous experiment
        self.clouds.selected_terminate()

        # Launch master and worker nodes
        self.master = Master(self.config, self.clouds)
        self.workers = Workers(self.config, self.clouds, self.master)

        # Wait until all workers register with master (as Idle resources)
        self.initialmonitor = InitialMonitor(self.config, self.master, len(self.workers.list))
        self.initialmonitor.start()
        self.initialmonitor.join()

        # Submit and execute workload
        self.workload = Workload(self.config, self.master)
        self.workload.execute()

        # Launch replacer and failure simulator
        self.replacer_stop= Event()
        self.replacer = Replacer(self.replacer_stop, self.config, self.master, interval=15)
        self.replacer.start()

        #self.failuresimulator_stop= Event()
        #self.failuresimulator = FailureSimulator(self.failuresimulator_stop, self.config, self.master)
        #self.failuresimulator = ExpFailureSimulator(self.failuresimulator_stop, self.config, self.master)
        #self.failuresimulator.start()

        self.simulators = []
        self.simulators_stops = []
        for group in self.config.workers.worker_groups:
            fs = ExpFailureSimulatorInOneCloud(self.failuresimulator_stop, self.config, self.master, group)
            self.simulators.append(fs)
            fs_stop = Event()
            self.simulators_stops.append(fs_stop)

        # Sleep while the replacer is running (while there are jobs in the queue)
        # Terminate failure simulators then
        while self.replacer.isAlive():
            time.sleep(5)

        for ind in range(len(self.simulators)):
            fs = self.simulators[ind]
            if fs.isAlive():
                fs_stop = self.simulators[ind]
                fs_stop.set()

        # Copy the master log back
        self.workload.get_log()

        # Terminate some instances
        self.clouds.selected_terminate()

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


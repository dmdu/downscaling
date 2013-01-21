import logging

from threading import Thread, Event
from resources.failuresimulator import FailureSimulator
from resources.aggressive import AggressiveDownscaler
from resources.opportunistic_idle import OpportunisticIdleDownscaler
from resources.opportunistic_offline import OpportunisticOfflineDownscaler

LOG = logging.getLogger(__name__)

class Policy(object):

    def __init__(self, config, master):

        self.config = config
        self.master = master
        self.running = False
        self.name = self.config.policy.policy_in_place

    def start(self):

        if self.name == "FAILURE":
            self.simulators = []
            self.simulators_stops = []
            for group in self.config.workers.worker_groups:
                fs_stop = Event()
                fs = FailureSimulator(fs_stop, self.config, self.master, group)
                fs.start()
                self.simulators.append(fs)
                self.simulators_stops.append(fs_stop)

        elif self.name == "OPPORTUNISTIC_IDLE":
            self.downscaler_stop = Event()
            self.downscaler = OpportunisticIdleDownscaler(self.downscaler_stop, self.config, self.master, self.config.downscaler_interval)
            self.downscaler.start()

        elif self.name == "OPPORTUNISTIC_OFFLINE":
            self.downscaler_stop = Event()
            self.downscaler = OpportunisticOfflineDownscaler(self.downscaler_stop, self.config, self.master, self.config.downscaler_interval)
            self.downscaler.start()

        elif self.name == "AGGRESSIVE":
            self.downscaler_stop = Event()
            self.downscaler = AggressiveDownscaler(self.downscaler_stop, self.config, self.master, self.config.downscaler_interval)
            self.downscaler.start()

        self.running = True

    def stop(self):

        if self.running:

            if self.name == "FAILURE":
                for ind in range(len(self.simulators)):
                    fs = self.simulators[ind]
                    if fs.isAlive():
                        fs_stop = self.simulators_stops[ind]
                        fs_stop.set()

            elif self.name == "OPPORTUNISTIC_IDLE":
                if self.downscaler.isAlive():
                    self.downscaler_stop.set()

            elif self.name == "OPPORTUNISTIC_OFFLINE":
                if self.downscaler.isAlive():
                    self.downscaler_stop.set()

            elif self.name == "AGGRESSIVE":
                if self.downscaler.isAlive():
                    self.downscaler_stop.set()

            self.running = False
        else:
            LOG.error("Trying to stop policy that is not running")
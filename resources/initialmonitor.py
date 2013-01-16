import logging
from threading import Thread
import time

from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class InitialMonitor(Thread):

    def __init__(self, config, master, expected_worker_count, interval=30):

        Thread.__init__(self)
        self.config = config
        self.master = master
        self.interval = interval
        self.expected_worker_count = expected_worker_count

    def run(self):

        time.sleep(120)
        LOG.info("Activating Initial Monitor. Expecting workers: %d, sleep period: %d sec"
                 % (self.expected_worker_count, self.interval))
        while True:
            time.sleep(self.interval)

            worker_count = self.idle_workers_count()
            if worker_count == self.expected_worker_count:
                LOG.info("%d worker(s) registered with the master and Idle (as expected). Terminating Initial Monitor"
                         % (worker_count))
                break
            elif worker_count > self.expected_worker_count:
                LOG.info("%d worker(s) registered and Idle. %d worker(s) expected. Terminating Initial Monitor"
                         % (worker_count, self.expected_worker_count))
                break
            else:
                LOG.info("%d out of %d worker(s) registered and Idle. Sleeping again"
                     % (worker_count, self.expected_worker_count))

    def idle_workers_count(self):

        command = "condor_status | grep Idle"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = command)
        rcmd.execute()
        out = rcmd.stdout

        if out == None:
            return 0
        else:
            items = out.split()
            item_count = len(items)
            # there are normally 8 items per line (i.e. per worker) in the condor_status output
            #print item_count
            if item_count%8 != 0:
                LOG.error("Number of items in the output of condor_status is not a multiple of 8")
            return item_count/8
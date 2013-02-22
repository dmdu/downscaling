import logging
from threading import Thread
from resources.clouds import Cloud
import time

from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class InitialMonitor(Thread):

    def __init__(self, config, master, expected_worker_count, interval=120):

        Thread.__init__(self)
        self.config = config
        self.master = master
        self.interval = interval
        self.expected_worker_count = expected_worker_count
        self.limit = int(self.config.globals.initial_monitor_time_limit)

    def run(self):

        time.sleep(240)
        self.start_timestamp = time.time()

        # No-change limit: if the worker_count does not change for this much, continue anyways
        nochange_limit = 25*60
        nochange_beginning = time.time()
        prev_worker_count = 0
        first_time_nochange = True

        LOG.info("Activating Initial Monitor. Expecting workers: %d, sleep period: %d sec, time limit: %d sec, no change limit: %d"
                 % (self.expected_worker_count, self.interval, self.limit, nochange_limit))
        while True:
            time.sleep(self.interval)

            worker_count = self.idle_workers_count()

            elapsed = time.time() - self.start_timestamp
            if elapsed > self.limit:
                LOG.info("%d worker(s) registered and Idle. %d worker(s) expected. Time limit has been exceeded"
                         % (worker_count, self.expected_worker_count))
                LOG.info("Terminating Unpropagated and Corrupted instances. Leaving only running instances")
                self.terminate_all_but_running_instances()
                break

            # No change control: make sure that this monitor does not "freeze" for longer than nochange_limit
            if prev_worker_count == worker_count:
                if first_time_nochange:
                    nochange_beginning = time.time()
                else:
                    first_time_nochange = False
                    nochange_elapsed = time.time() - nochange_beginning
                    if nochange_elapsed > nochange_limit:
                        LOG.info("%d worker(s) registered and Idle. %d worker(s) expected. No-change limit has been exceeded"
                                 % (worker_count, self.expected_worker_count))
                        LOG.info("Terminating Unpropagated and Corrupted instances. Leaving only running instances that are in the pool")
                        self.terminate_all_but_running_instances()
                        break
            else:
                prev_worker_count = worker_count
                first_time_nochange = True

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

    def terminate_all_but_running_instances(self):

        for cloud_name in self.config.clouds.list:
            cloud = Cloud(cloud_name, self.config)
            cloud.terminate_all_but_running_instances()

        time.sleep(30)
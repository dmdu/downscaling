import logging
from threading import Thread
import time

from lib.util import RemoteCommand
from resources.jobs import Job, Jobs

LOG = logging.getLogger(__name__)

class Monitor(Thread):

    def __init__(self, config, master, interval=20):

        Thread.__init__(self)
        self.config = config
        self.master = master
        self.interval = interval

    def run(self):

        LOG.info("Activating Monitor. Sleep period: %d sec" % (self.interval))
        while True:
            time.sleep(self.interval)

            jobs = self.get_running_jobs()
            if len(jobs.list) == 0:
                LOG.info("No jobs in the queue. Terminating Monitor")
                break

    def get_running_jobs(self):

        command = "condor_q -run | grep %s" % (self.config.workload.user)
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = command)
        rcmd.execute()
        jobs = Jobs(rcmd.stdout, self.config.workload.user)
        return jobs

    def get_current_workers(self):

        command = "condor_status"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = command)
        rcmd.execute()
        jobs = Jobs(rcmd.stdout, self.config.workload.user)
        return jobs
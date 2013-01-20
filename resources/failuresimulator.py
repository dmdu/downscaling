import logging
import random
import os
import time

from threading import Thread
from resources.clouds import Cloud
from lib.util import RemoteCommand
from lib.logger import filelog
from resources.workers import Worker

LOG = logging.getLogger(__name__)

class FailureSimulator(Thread):
    """
    Terminate one VM at a time using exponential failure distribution within a given cloud (group specifies this cloud)
    """

    def __init__(self, stop_event, config, master, group):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.group = group
        random.seed(os.urandom(128))

    def run(self):

        self.cloud_name = self.group['cloud']

        if self.group['failure_rate'] == "None":
            # No failures, shut the simulator
            LOG.info("Failure-Simulator-%s: failure rate is set to None. Terminating simulator" % (self.cloud_name))
            self.stop_event.set()
            return

        self.failute_rate = float(self.group['failure_rate'])
        self.interval = random.expovariate(self.failute_rate)

        while(not self.stop_event.is_set()):
            LOG.info("Failure-Simulator-%s: sleeping for %d sec" % (self.cloud_name, self.interval))
            self.stop_event.wait(self.interval)

            list_of_vms = self.get_cloud_termination_list()

            # continue as normal
            count = len(list_of_vms)
            if count > 0:
                pick = random.randint(0, count-1)
                instance = list_of_vms[pick]

                LOG.info("Failure-Simulator-%s: terminating an instance %s (%s)"
                         % (self.cloud_name, instance.id, instance.public_dns_name))
                filelog(self.config.failure_log, "%s,TERMINATED,%s,%s"
                                                 % (time.time(), self.cloud_name, instance.public_dns_name))

                worker = Worker(self.config, instance)
                worker.terminate() # terminates condor daemon and shuts down instance

                # Reset the sleep time (interval before the next failure)
                self.interval = random.expovariate(self.failute_rate)

    def get_cloud_termination_list(self):

        condor_list = []
        command = "condor_status"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = command)
        rcmd.execute()
        if rcmd.stdout:
            # condor status will be lines so split them
            all_lines = rcmd.stdout.split("\n")
            for line in all_lines:
                # line not empty
                if line.strip():
                # if its the first line then go to the next one
                    if line.strip().startswith("Name"):
                        continue
                    # if we find a line that starts with total then we are done, break out from the loop
                    elif line.strip().startswith("Total"):
                        break
                    # it must be a line of interest, parse it
                    else:
                        # split line by space :
                        #"vm-148-102.uc.futu LINUX      X86_64 Unclaimed Idle     0.150  2048  0+00:00:04"
                        line_columns = line.split()
                        try:
                            tmp_fqdn = line_columns[0].strip()
                            condor_list.append(tmp_fqdn)
                        except Exception as expt:
                            LOG.info("Error parsing condor status, line says : %s and the expt says : %s" % (line, str(expt)))
        LOG.info("Condor worker names: %s" % (str(condor_list)))

        # instances running in this cloud
        cloud = Cloud(self.cloud_name, self.config)
        vms = cloud.get_running_instances()

        # add to the termination list only workers (no master) that have checked in with the master:
        termination_list = []
        for instance in vms:

            # not a master
            if not instance.public_dns_name == self.master.dns:

                for worker_partial_name in condor_list:
                    if worker_partial_name in instance.public_dns_name:
                        termination_list.append(instance)
                        condor_list.remove(worker_partial_name)

        termination_list_names =[]
        for instance in termination_list:
            termination_list_names.append(instance.public_dns_name)
        LOG.info("Termination list for %s: %s" % (self.cloud_name, str(termination_list_names)))

        return termination_list
import logging
from threading import Thread
import time
import datetime

from lib.util import RemoteCommand
from resources.jobs import Job, Jobs
from lib.logger import filelog

LOG = logging.getLogger(__name__)

class Monitor(Thread):

    def __init__(self, config, master, workers, interval=10):

        Thread.__init__(self)
        self.config = config
        self.master = master
        self.interval = interval
        self.workers = workers

    def run(self):

        LOG.info("Activating Monitor. Sleep period: %d sec" % (self.interval))
        while True:
            time.sleep(self.interval)

            jobs = self.get_running_jobs()

            #workers_dns_list = self.query_current_workers()
            worker_pool, worker_pool_str = self.match_workers_to_cloud()
            print worker_pool_str
            filelog(self.config.worker_pool_log, worker_pool_str)

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

    def query_current_workers(self):
        """ Connect to master, get list of all workers hostname and return it"""

        workers_dns_list = []

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
                            workers_dns_list.append(tmp_fqdn)
                        except Exception as expt:
                            LOG.info("Error parsing condor status, line says : %s and the expt says : %s" % (line, str(expt)))

        return workers_dns_list

#    def match_workers_to_cloud(self):
#        """
#        I found out that hotels hostname always looks like vm-148-103.uc.futuregrid.org and sierra looks like
#        vm-9.sdsc.futuregrid.org. Also condor status always return at least first and second parts of the hostname,
#        "vm-148-102.uc.futu" so I used this information to determine which cloud a vm belong to.
#        """
#        current_workers = self.query_current_workers()
#
#        clouds_dict = {}
#        for acloud in self.config.clouds.list:
#            clouds_dict[acloud] = 0
#
#        for vms_fqdn in current_workers:
#            try:
#                fqdn_parts = vms_fqdn.split(".")
#                if fqdn_parts[1] == "uc":
#                    clouds_dict["hotel"] += 1
#                elif fqdn_parts[1] == "sdsc":
#                    clouds_dict["sierra"] += 1
#                else:
#                    LOG.info("Got strange hostname from condor status, line says : %s" % (vms_fqdn))
#            except Exception as expt:
#                LOG.info("Error parsing condor status, line says : %s and the expt says : %s" % (vms_fqdn, str(expt)))
#
#        return {time.time():clouds_dict}


    def match_workers_to_cloud(self):
        """
        This version of the same functions depends on provided workers object and rely on workers.cloud_to_instance_dns_list['hotel']
        """

        current_workers = self.query_current_workers()
        current_workers_two_parts = [ ".".join(x.split(".")[:2]) for x in current_workers]
        clouds_dict = {}

        for acloud in self.config.clouds.list:
            clouds_dict[acloud] = 0
            vms_dns = self.workers.cloud_to_instance_dns_list[acloud]
            vms_dns_two_parts = [ ".".join(x.split(".")[:2]) for x in vms_dns]

            for worker in current_workers_two_parts:
                if worker in vms_dns_two_parts:
                    clouds_dict[acloud] += 1

        timestamp = time.time()
        result = {timestamp:clouds_dict}
        str_format = "%s," % (timestamp)
        for cloud_name, instance_count in clouds_dict.iteritems():
            str_format += "%s:%s," % (cloud_name,str(instance_count))
        # remove last char
        str_format = str_format[:-1]
        return result, str_format

        #return {time.time():clouds_dict}
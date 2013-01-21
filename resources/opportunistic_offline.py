import logging
import random
import os
import copy
import operator

from threading import Thread
from resources.clouds import Cloud
from lib.util import RemoteCommand
from resources.jobs import Jobs
from lib.logger import filelog
from resources.workers import Worker

LOG = logging.getLogger(__name__)

class OpportunisticOfflineDownscaler(Thread):

    def __init__(self, stop_event, config, master, interval=120):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        self.get_desired_dict()

    def run(self):

        LOG.info("Activating OO. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)

            curr_dict = self.get_current_dict()
            jobs = self.get_running_jobs()
            for cloud_name in curr_dict:
                if curr_dict[cloud_name] > self.desired_dict[cloud_name]:
                    diff = curr_dict[cloud_name] - self.desired_dict[cloud_name]
                    idle_candidates, nonidle_candidates = self.get_candidates(cloud_name, jobs, diff)

                    for instance in idle_candidates:
                        dns = instance.public_dns_name
                        LOG.info("OO terminated idle instance %s in %s" % (cloud_name, instance.id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d" % (cloud_name, dns, 0))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance.id, dns))
                        worker = Worker(self.config, instance)
                        worker.terminate() # terminates condor daemon and shuts down instance
                    for instance in nonidle_candidates:
                        dns = instance.public_dns_name
                        LOG.info("OO marked instance offline %s in %s" % (cloud_name, instance.id))
                        filelog(self.config.node_log, "OFFLINED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance.id, dns))
                        worker = Worker(self.config, instance)
                        worker.offline(self.master.dns) # marks node offline (it later becomes idle and get terminated)


    def get_desired_dict(self):
        # assigns both the total count and the desired dict (by cloud)
        self.desired_dict = {}
        for group in self.config.workers.worker_groups:
            count = int(group['desired'])
            self.desired_dict[group['cloud']] = count
        LOG.info("OO determined desired dictionary: %s" % (str(self.desired_dict)))
        return

    def get_current_dict(self):

        pool_dict = {}
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            # we only care about worker instances here, so don't count the master
            count = len(cloud.get_instances(exclude_dns=self.master.dns))
            pool_dict[acloud] = count
        LOG.info("OO found current instance dictionary: %s" % (str(pool_dict)))
        return pool_dict

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

    def get_candidates(self, cloud_name, jobs, count):
        """ Returns two lists of instances that should be terminated.
            idle_list: if there are any idle instances they will be returned in this list (and can be hard terminated).
            nonidle_list: if count > number of idle instances, then this list will include non-idle instances
                that have been running their job the longest (which means, they must be closer to completion;
                these instances should be marked offline.
            len(idle_list) + len(nonidle_list) should be = count;
            At the same time, either list can be empty
        """

        cloud = Cloud(cloud_name, self.config)
        # we only care about worker instances here, so don't include the master
        instances = cloud.get_instances(exclude_dns=self.master.dns)
        localjobs = copy.copy(jobs)

        idle_list = []
        nonidle_list = []
        for instance in instances:
            job_matching_found = False
            for job in localjobs.list:
                if instance.public_dns_name == job.node:
                    nonidle_list.append( (instance, job.running) )
                    localjobs.list.remove(job)
                    job_matching_found = True
                    break
            if not job_matching_found:
                idle_list.append(instance)

        # Truncate idle list if needed (in case there are more idle instances than count)
        # Does not do anything if count >= len(idle_list)
        idle_list = idle_list[:count]

        if idle_list:
            idle_list_str = ""
            for instance in idle_list:
                idle_list_str += "%s:%s," % (instance.id, instance.public_dns_name)
            LOG.info("OO found idle candidates for termination in %s: %s" % (cloud_name, idle_list_str))

        # Sort by the run time in the decreasing order
        sorted_nonidle_list = sorted(nonidle_list, key=operator.itemgetter(1), reverse=True)

        remaining_count = count - len(idle_list)
        # Truncate sorted non-idle list if needed (in case remaining_count < len(sorted_nonidle_list))
        sorted_nonidle_list = sorted_nonidle_list[:remaining_count]

        sorted_nonidle_list_instances_only = []
        if sorted_nonidle_list:
            nonidle_list_str = ""
            for atuple in sorted_nonidle_list:
                nonidle_list_str += "%s:%s:%s," % (atuple[0].id, atuple[0].public_dns_name, atuple[1])
                sorted_nonidle_list_instances_only.append(atuple[0])
            LOG.info("OO found non-idle candidates for termination in %s: %s" % (cloud_name, nonidle_list_str))

        total_found = len(idle_list)+len(sorted_nonidle_list_instances_only)
        if not total_found == count:
            LOG.info("OO can't supply enough (%d) instances for termination. Found only %d", count, total_found)

        return idle_list, sorted_nonidle_list_instances_only
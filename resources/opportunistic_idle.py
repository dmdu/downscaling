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

class OpportunisticIdleDownscaler(Thread):

    def __init__(self, stop_event, config, master, interval=120):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        self.get_desired_dict()

    def run(self):

        LOG.info("Activating OI. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)

            curr_dict = self.get_current_dict()
            jobs = self.get_running_jobs()
            for cloud_name in curr_dict:
                if curr_dict[cloud_name] > self.desired_dict[cloud_name]:
                    diff = curr_dict[cloud_name] - self.desired_dict[cloud_name]
                    candidates = self.get_idle_instances(cloud_name, jobs)

                    # Only terminate as many as needed
                    termination_list = candidates[:diff]
                    for instance in termination_list:
                        dns = instance.public_dns_name
                        LOG.info("OI terminated instance %s in %s" % (cloud_name, instance.id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d" % (cloud_name, dns, 0))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance.id, dns))
                        worker = Worker(self.config, instance)
                        worker.terminate() # terminates condor daemon and shuts down instance


    def get_desired_dict(self):
        # assigns both the total count and the desired dict (by cloud)
        self.desired_dict = {}
        for group in self.config.workers.worker_groups:
            count = int(group['desired'])
            self.desired_dict[group['cloud']] = count
        LOG.info("OI determined desired dictionary: %s" % (str(self.desired_dict)))
        return

    def get_current_dict(self):

        pool_dict = {}
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            # we only care about worker instances here, so don't count the master
            count = len(cloud.get_instances(exclude_dns=self.master.dns))
            pool_dict[acloud] = count
        LOG.info("OI found current instance dictionary: %s" % (str(pool_dict)))
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

    def get_idle_instances(self, cloud_name, jobs):

        cloud = Cloud(cloud_name, self.config)
        # we only care about worker instances here, so don't include the master
        instances = cloud.get_instances(exclude_dns=self.master.dns)
        localjobs = copy.copy(jobs)

        idle_instances = []
        for instance in instances:
            job_matching_found = False
            for job in localjobs.list:
                if instance.public_dns_name == job.node:
                    job_matching_found = True
                    localjobs.list.remove(job)
                    break
            if not job_matching_found:
                idle_instances.append(instance)
                LOG.info("OI found an idle instance: %s. Selected it for termination" % (instance.public_dns_name))
        return idle_instances


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

LOG = logging.getLogger(__name__)

class OpportunisticDownscalerA(Thread):

    def __init__(self, stop_event, config, master, interval=120):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        self.get_desired_dict()

    def run(self):

        LOG.info("Activating ODA. Sleep period: %d sec" % (self.interval))
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
                        LOG.info("ODA terminated instance %s in %s" % (cloud_name, instance.id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d" % (cloud_name, dns, 0))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, reservation: %s, instance: %s, dns: %s"
                                                      % (cloud_name, "reservation-TBD", instance.id, dns))
                        self.stop_condor(dns)
                        instance.terminate()


    def get_desired_dict(self):
        # assigns both the total count and the desired dict (by cloud)
        self.desired_dict = {}
        for group in self.config.workers.worker_groups:
            count = int(group['desired'])
            self.desired_dict[group['cloud']] = count
        LOG.info("ODA determined desired dictionary: %s" % (str(self.desired_dict)))
        return

    def get_current_dict(self):

        pool_dict = {}
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            # we only care about worker instances here, so don't count the master
            count = len(cloud.get_instances(exclude_dns=self.master.dns))
            pool_dict[acloud] = count
        LOG.info("ODA found current instance dictionary: %s" % (str(pool_dict)))
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
                LOG.info("ODA found an idle instance: %s. Selected it for termination" % (instance.public_dns_name))
        return idle_instances

    def get_cloud_instances_by_runtime(self, cloud_name, jobs):

        cloud = Cloud(cloud_name, self.config)
        # we only care about worker instances here, so don't include the master
        instances = cloud.get_instances(exclude_dns=self.master.dns)
        localjobs = copy.copy(jobs)

        instances_by_runtime = []
        for instance in instances:
            for job in localjobs.list:
                if instance.public_dns_name == job.node:
                    instances_by_runtime.append( (instance, job.running) )
                    localjobs.list.remove(job)
                    break

        sorted_instances_by_runtime = sorted(instances_by_runtime, key=operator.itemgetter(1))

        # Logging
        sorted_list_str = ""
        for atuple in sorted_instances_by_runtime:
            sorted_list_str += "%s:%s," % (atuple[0].id, atuple[1])
        LOG.info("Candidates for termination in %s: %s" % (cloud_name, sorted_list_str))

        return sorted_instances_by_runtime

    def select_from_candidates(self, cloud_name, candidates, count_needed):
        """
        2-stage selection:
        first, out of all candidates (instance, running time) we select only count_needed first candidates
        (with the shortest runtimes)
        second, out of selected instances we form a list with instances that have accomplished less work then
        specified threshold
        """

        if count_needed <= len(candidates):
            LOG.info("ODA: selecting %d out of %d instances for termination in %s. Enough candidates found"
                     % (count_needed, len(candidates), cloud_name))
        else:
            LOG.info("ODA: selecting %d out of %d instances for termination in %s. Not enough candidates"
                     % (count_needed, len(candidates), cloud_name))

        first_stage_candidates = candidates[:count_needed]

        # Select candidates with less than threshold amount of work accomplished
        second_stage_candidates = []
        for candidate in first_stage_candidates:
            if candidate[1] < self.config.threshold:
                second_stage_candidates.append(candidate)

        return second_stage_candidates

    def stop_condor(self, dns):

        command = "/etc/init.d/condor stop"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = dns,
            ssh_private_key = self.config.globals.priv_path,
            user = 'root',
            command = command)
        code = rcmd.execute()
        if code == 0:
            LOG.info("Successfully stopped Condor daemon on instance: %s" % (dns))
        else:
            LOG.error("Error occurred during Condor daemon termination on instance: %s" % (dns))
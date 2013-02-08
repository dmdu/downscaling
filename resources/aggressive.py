import logging
import copy
import operator
import time

from threading import Thread
from resources.jobs import Jobs
from lib.logger import filelog
from resources.workers import Worker


LOG = logging.getLogger(__name__)

class AggressiveDownscaler(Thread):

    def __init__(self, stop_event, config, master, phantom_client, interval=120):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        self.get_desired_dict()
        self.phantom_client = phantom_client

    def run(self):

        LOG.info("Activating AD. Sleep period: %d sec" % (self.interval))
        jobs = Jobs(self.config, self.master.dns)
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)

            curr_dict = self.get_current_dict()
            jobs.update_current_list()

            pool_dict_str = "%s," % (time.time())
            for cloud_name, instance_count in curr_dict.iteritems():
                pool_dict_str += "%s:%d," % (cloud_name,instance_count)
            pool_dict_str = pool_dict_str[:-1]
            filelog(self.config.worker_pool_log, pool_dict_str)


            for cloud_name in curr_dict:
                if curr_dict[cloud_name] > self.desired_dict[cloud_name]:
                    diff = curr_dict[cloud_name] - self.desired_dict[cloud_name]
                    candidates = self.get_cloud_instances_by_runtime_inc(cloud_name, jobs)
                    termination_list = self.select_from_candidates(cloud_name, candidates, diff)
                    for atuple in termination_list:
                        instance_id = atuple[0]
                        running = atuple[1]
                        instance_info = atuple[2]

                        dns = instance_info['public_dns']

                        LOG.info("AD terminated instance %s in %s" % (cloud_name, instance_id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%s" % (cloud_name, dns, running))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance_id, dns))


                        Worker(self.config, instance_id, instance_info).terminate_condor()
                        self.phantom_client.terminate_instance(instance_id)

    def get_desired_dict(self):
        # assigns both the total count and the desired dict (by cloud)
        self.desired_dict = {}
        for group in self.config.workers.worker_groups:
            count = int(group['desired'])
            self.desired_dict[group['cloud']] = count
        LOG.info("AD determined desired dictionary: %s" % (str(self.desired_dict)))
        return

    def get_current_dict(self):


        pool_dict = {}
        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances_info = self.phantom_client.get_alive_instnaces(all_instances_info)

        for acloud in self.config.clouds.list:
            pool_dict[acloud] = 0
            for instance_id in instances_info:
                if instances_info[instance_id]['cloud_name'] == acloud:
                    pool_dict[acloud] += 1
        LOG.info("AD found current instance dictionary: %s" % (str(pool_dict)))


        return pool_dict

    def get_cloud_instances_by_runtime_inc(self, cloud_name, jobs):
        """ Return instances in the cloud sorted by the time they have been running their jobs (increasing order) """

        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances = self.phantom_client.get_alive_instnaces(all_instances_info)

        localjobs = copy.copy(jobs)

        instances_by_runtime = []
        for instance in instances:
            for job in localjobs.list:
                if instances[instance]['public_dns'] == job.node:
                    instances_by_runtime.append( (instance, job.running, instances[instance]) )
                    localjobs.list.remove(job)
                    break

        sorted_instances_by_runtime = sorted(instances_by_runtime, key=operator.itemgetter(1))

        # Logging
        sorted_list_str = ""
        for atuple in sorted_instances_by_runtime:
            sorted_list_str += "%s:%s," % (atuple[0], atuple[1])
        LOG.info("AD found candidates for termination in %s: %s" % (cloud_name, sorted_list_str))

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
            LOG.info("AD: selecting %d out of %d instances for termination in %s. Enough candidates found"
                     % (count_needed, len(candidates), cloud_name))
        else:
            LOG.info("AD: selecting %d out of %d instances for termination in %s. Not enough candidates"
                     % (count_needed, len(candidates), cloud_name))

        # Truncate if needed (in cases  when count_needed < len(candidates)
        first_stage_candidates = candidates[:count_needed]

        # Select candidates with less than threshold amount of work accomplished
        second_stage_candidates = []
        for candidate in first_stage_candidates:
            if candidate[1] < self.config.threshold:
                second_stage_candidates.append(candidate)

        return second_stage_candidates
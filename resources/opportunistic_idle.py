import logging
import copy
import time

from threading import Thread
from resources.jobs import Jobs
from lib.logger import filelog
from resources.workers import Worker

LOG = logging.getLogger(__name__)

class OpportunisticIdleDownscaler(Thread):

    def __init__(self, stop_event, config, master, phantom_client, interval=120):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        self.get_desired_dict()
        self.phantom_client = phantom_client

    def run(self):

        LOG.info("Activating OI. Sleep period: %d sec" % (self.interval))
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
                    candidates = self.get_idle_instances(cloud_name, jobs)

                    # Only terminate as many as needed
                    termination_list = candidates[:diff]
                    for instance_tuple in termination_list:
                        instance_id = instance_tuple[0]
                        instance_info = instance_tuple[1]
                        dns = instance_info['public_dns']
                        LOG.info("OI terminated instance %s in %s" % (cloud_name, instance_id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d" % (cloud_name, dns, 0))
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
        LOG.info("OI determined desired dictionary: %s" % (str(self.desired_dict)))
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
        LOG.info("OI found current instance dictionary: %s" % (str(pool_dict)))


        return pool_dict


    def get_idle_instances(self, cloud_name, jobs):

        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances = self.phantom_client.get_alive_instnaces(all_instances_info)


        localjobs = copy.copy(jobs)

        idle_instances = []
        for instance in instances:
            job_matching_found = False
            for job in localjobs.list:
                if instances[instance]['public_dns'] == job.node:
                    job_matching_found = True
                    localjobs.list.remove(job)
                    break
            if not job_matching_found:
                idle_instances.append( (instance, instances[instance]) )
                LOG.info("OI found an idle instance: %s. Selected it for termination" % (instances[instance]['public_dns']))
        return idle_instances


import logging
import copy
import time
import operator

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

        time.sleep(20) # To allow jobs to be added to the queue

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

            diff_dict = {}

            for cloud_name in curr_dict:
                up_diff =  self.desired_dict[cloud_name] - curr_dict[cloud_name]
                diff_dict[cloud_name] = up_diff

            for cloud_name in curr_dict:
                if curr_dict[cloud_name] > self.desired_dict[cloud_name]:

                    down_diff = - diff_dict[cloud_name]
                    candidates = self.get_idle_instances(cloud_name, jobs)

                    # Only terminate as many as needed
                    termination_list = candidates[:down_diff]
                    if termination_list:
                        LOG.info("Downscaling in %s" % (cloud_name))
                    else:
                        LOG.info("Not Downscaling because no idle instances found in %s" % (cloud_name))

                    for instance_tuple in termination_list:
                        instance_id = instance_tuple[0]
                        instance_info = instance_tuple[1]
                        dns = instance_info['public_dns']
                        LOG.info("OI terminated instance %s in %s" % (cloud_name, instance_id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d" % (cloud_name, dns, 0))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance_id, dns))

                        LOG.info("Desired capacity (before termination) is %d" % (self.phantom_client.asg.desired_capacity))

                        Worker(self.config, instance_id, instance_info).terminate_condor(self.master.dns)
                        self.phantom_client.terminate_instance(instance_id)

                        LOG.info("Desired capacity (after termination) is %d" % (self.phantom_client.asg.desired_capacity))

                        # upscale

                        sorted_diff_dict = sorted(diff_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
                        if sorted_diff_dict[0][1] > 0:
                            cloud_to_upscale = sorted_diff_dict[0][0]
                            if cloud_to_upscale != cloud_name:
                            # create new tag :
                                current_cloud_tag = self.phantom_client.cloud_list.split(",")
                                new_cloud_tag = ""
                                new_cloud_count = 0
                                LOG.info("Current cloud tag is %s" % (self.phantom_client.cloud_list))
                                LOG.info("Current dict is %s" % (str(curr_dict)))
                                LOG.info("Diff dict is %s" % (str(diff_dict)))
                                for each_cloud in current_cloud_tag:
                                    tmp_cloud_name = each_cloud.split(":")[0]
                                    tmp_cloud_count = int(each_cloud.split(":")[1])
                                    if tmp_cloud_name == cloud_to_upscale:
                                        new_cloud_tag += "%s:%d," % (tmp_cloud_name, tmp_cloud_count +1)
                                        curr_dict[tmp_cloud_name] += 1
                                        diff_dict[tmp_cloud_name] -= 1
                                    elif tmp_cloud_name == cloud_name:
                                        new_cloud_tag += "%s:%d," % (tmp_cloud_name, tmp_cloud_count - 1)
                                        curr_dict[tmp_cloud_name] -= 1
                                        diff_dict[tmp_cloud_name] += 1
                                    else:
                                        new_cloud_tag += "%s:%d," % (tmp_cloud_name, tmp_cloud_count)
                                    new_cloud_count += curr_dict[tmp_cloud_name]

                                new_cloud_tag_no_comma = new_cloud_tag[:-1]
                                LOG.info("New cloud tag is %s" % (new_cloud_tag_no_comma))
                                LOG.info("New Current dict is %s" % (str(curr_dict)))
                                LOG.info("New Diff dict is %s" % (str(diff_dict)))
                                LOG.info("New Desired capacity (after recounting) is %d" % (new_cloud_count))

                                self.phantom_client.update_tags(new_cloud_tag_no_comma, new_cloud_count)
                                self.phantom_client.cloud_list = new_cloud_tag_no_comma
                                self.phantom_client.asg.set_capacity(new_cloud_count)
                            else:
                                LOG.info("Trying to upscale and downscale in the same cloud .. STOPPED")


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
            if instances[instance]['cloud_name'] != cloud_name:
                continue
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


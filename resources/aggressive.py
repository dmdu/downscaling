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
                    LOG.info("Downscaling in %s" % (cloud_name))
                    down_diff = - diff_dict[cloud_name]
                    candidates = self.get_cloud_instances_by_runtime_inc(cloud_name, jobs)
                    termination_list = self.select_from_candidates(cloud_name, candidates, down_diff)
                    for atuple in termination_list:
                        instance_id = atuple[0]
                        running = atuple[1]
                        instance_info = atuple[2]
                        progress = atuple[3]

                        dns = instance_info['public_dns']

                        LOG.info("AD terminated instance %s in %s" % (cloud_name, instance_id))
                        filelog(self.config.discarded_work_log, "DISCARDED,%s,%s,%d,%f" % (cloud_name, dns, running, progress))
                        filelog(self.config.node_log, "TERMINATED WORKER cloud: %s, instance: %s, dns: %s"
                                                      % (cloud_name, instance_id, dns))

                        LOG.info("Desired capacity (before termination) is %d" % (self.phantom_client.asg.desired_capacity))
                        Worker(self.config, instance_id, instance_info).terminate_condor(self.master.dns)
                        self.phantom_client.terminate_instance(instance_id)
                        LOG.info("Desired capacity (after termination) is %d" % (self.phantom_client.asg.desired_capacity))

                        # figure out where to up scale

                        # sort the diff dict to find cloud with max number of lack isntances ( up scale )
                        # [('c', 10), ('a', 3), ('b', 1)]
                        # we sort dict by value, it returns list of tuples
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

         # ATTENTION: This function has nothing to do with the runtime anymore
         # It sorts jobs by their progress = runtime/walltime (if available)

        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances = self.phantom_client.get_alive_instnaces(all_instances_info)

        localjobs = copy.copy(jobs)


        instances_by_runtime = []
        for instance in instances:
            if instances[instance]['cloud_name'] != cloud_name:
                continue
            job_matching_found = False
            for job in localjobs.list:
                if instances[instance]['public_dns'] == job.node:

                    #instances_by_runtime.append( (instance, job.running, instances[instance]) )
                    instances_by_runtime.append( (instance, job.running, instances[instance], job.progress) )

                    localjobs.list.remove(job)
                    job_matching_found = True
                    break
            if not job_matching_found:
                # idle
                instances_by_runtime.append( (instance, 0, instances[instance], 0.0) )

        #sorted_instances_by_runtime = sorted(instances_by_runtime, key=operator.itemgetter(1))
        sorted_instances_by_runtime = sorted(instances_by_runtime, key=operator.itemgetter(3)) # sort by progress

        # Logging
        sorted_list_str = ""
        for atuple in sorted_instances_by_runtime:
            sorted_list_str += "%s:%d:%f," % (atuple[0], atuple[1], atuple[3])
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
            # check progress (not running)
            if float(candidate[3]) < float(self.config.threshold):
                second_stage_candidates.append(candidate)

        return second_stage_candidates

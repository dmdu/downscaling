import logging
import copy
import operator
import time

from threading import Thread
from resources.jobs import Jobs
from lib.logger import filelog
from resources.workers import Worker

LOG = logging.getLogger(__name__)

class OpportunisticOfflineDownscaler(Thread):

    def __init__(self, stop_event, config, master, phantom_client, interval=120, check_within_interval_limit=6):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = int(float(interval)/check_within_interval_limit)
        self.phantom_client = phantom_client
        self.get_desired_dict()

        self.check_within_interval_limit = check_within_interval_limit
        self.check_within_interval_counter = 0
        self.marked_offline_list = []
        self.marked_offline_list_for_sure_empty = True

    def run(self):

        LOG.info("Activating OO. Sleep period: %d sec" % (self.interval))
        jobs = Jobs(self.config, self.master.dns)

        while(not self.stop_event.is_set()):

            self.stop_event.wait(self.interval)

            # Figure out what type of iteration this is:
            # - either when the counter equals the limit -- then try marking nodes offline
            # - any other iteration -- then only terminate idle instances if any in the marked_offline_list
            self.check_within_interval_counter += 1
            if self.check_within_interval_counter == self.check_within_interval_limit:
                allow_marking_offline = True
                self.marked_offline_list_for_sure_empty = False
                # Rest and go back to the beginning of the cycle
                self.check_within_interval_counter = 0
                LOG.info("OO's iteration with marking nodes offline and termination of idle instances")
            else:
                allow_marking_offline = False
                LOG.info("OO's iteration with termination of previously marked instances. Iteration: %d of %d" 
                              % (self.check_within_interval_counter, self.check_within_interval_limit))

            curr_dict = self.get_current_dict()
            jobs.update_current_list()

            pool_dict_str = "%s," % (time.time())
            for cloud_name, instance_count in curr_dict.iteritems():
                pool_dict_str += "%s:%d," % (cloud_name,instance_count)
            pool_dict_str = pool_dict_str[:-1]
            filelog(self.config.worker_pool_log, pool_dict_str)

            if self.marked_offline_list_for_sure_empty:
                LOG.info("Empty list. Continuing")
                continue
                
            diff_dict = {}

            for cloud_name in curr_dict:
                up_diff =  self.desired_dict[cloud_name] - curr_dict[cloud_name]
                diff_dict[cloud_name] = up_diff


            for cloud_name in curr_dict:
                if curr_dict[cloud_name] > self.desired_dict[cloud_name]:
                    down_diff = - diff_dict[cloud_name]
                    LOG.info("Downscaling in %s, trying to downscale %d instance(s)." % (cloud_name, down_diff))

                    if not allow_marking_offline:
                        # give me all idle_instances that are in self.marked_offline_list
                        # only these instances are allowed to be terminated
                        if self.marked_offline_list:
                            candidates = self.get_candidates(cloud_name, jobs, down_diff, return_only_all_idle=True)
                            idle_candidates = []
                            for cand in candidates:
                                ins_id = cand[0]
                                if ins_id in self.marked_offline_list:
                                    idle_candidates.append(cand)
                                    LOG.info("Selecting idle offline instance for termination: %s" % (ins_id))
                                    self.marked_offline_list.remove(ins_id)
                        else:
                            idle_candidates = []
                    else:
                        idle_candidates, nonidle_candidates = self.get_candidates(cloud_name, jobs, down_diff, return_only_all_idle=False)

                    for instance_tuple in idle_candidates:
                        instance_id = instance_tuple[0]
                        instance_info = instance_tuple[1]
                        dns = instance_info['public_dns']
                        LOG.info("OO terminated idle instance %s in %s" % (cloud_name, instance_id))
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

                    if allow_marking_offline:
                        for instance_tuple in nonidle_candidates:
                            instance_id = instance_tuple[0]
                            instance_info = instance_tuple[1]
                            dns = instance_info['public_dns']
                            LOG.info("OO marked instance offline %s in %s" % (cloud_name, instance_id))
                            filelog(self.config.node_log, "OFFLINED WORKER cloud: %s, instance: %s, dns: %s"
                                                          % (cloud_name, instance_id, dns))
                            filelog(self.config.discarded_work_log, "OFFLINE,%s,%s,%d" % (cloud_name, dns, 0))

                            worker = Worker(self.config, instance_id, instance_info)
                            worker.offline(self.master.dns) # marks node offline (it later becomes idle and get terminated)
                            self.marked_offline_list.append(instance_id)


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
        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances_info = self.phantom_client.get_alive_instnaces(all_instances_info)

        for acloud in self.config.clouds.list:
            pool_dict[acloud] = 0
            for instance_id in instances_info:
                if instances_info[instance_id]['cloud_name'] == acloud:
                    pool_dict[acloud] += 1
        LOG.info("OO found current instance dictionary: %s" % (str(pool_dict)))


        return pool_dict


    def get_candidates(self, cloud_name, jobs, count, return_only_all_idle=False):
        """ Returns two lists of instances that should be terminated.
            idle_list: if there are any idle instances they will be returned in this list (and can be hard terminated).
            nonidle_list: if count > number of idle instances, then this list will include non-idle instances
                that have been running their job the longest (which means, they must be closer to completion;
                these instances should be marked offline.
            len(idle_list) + len(nonidle_list) should be = count;
            At the same time, either list can be empty
        """

        asg_info = self.phantom_client.get_autoscale_groups_info(self.phantom_client.asg.name)
        all_instances_info = asg_info[self.phantom_client.asg.name]['instances']
        instances = self.phantom_client.get_alive_instnaces(all_instances_info)

        localjobs = copy.copy(jobs)

        idle_list = []
        nonidle_list = []
        for instance in instances:
            if instances[instance]['cloud_name'] != cloud_name:
                continue
            job_matching_found = False
            for job in localjobs.list:
                if instances[instance]['public_dns'] == job.node:
                    nonidle_list.append( (instance, job.running, instances[instance]) )
                    localjobs.list.remove(job)
                    job_matching_found = True
                    break
            if not job_matching_found:
                idle_list.append( (instance, instances[instance]) )

        # Truncate idle list if needed (in case there are more idle instances than count)
        # Does not do anything if count >= len(idle_list)

        if return_only_all_idle:
            # DONE if this flag is set
            return idle_list

        idle_list = idle_list[:count]

        if idle_list:
            idle_list_str = ""
            for instance in idle_list:
                idle_list_str += "%s:%s," % (instance[0], instance[1]['public_dns'])
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
                nonidle_list_str += "%s:%s:%s," % (atuple[0], atuple[2]['public_dns'], atuple[1])
                sorted_nonidle_list_instances_only.append((atuple[0], atuple[2] ))
            LOG.info("OO found non-idle candidates for termination in %s: %s" % (cloud_name, nonidle_list_str))

        total_found = len(idle_list)+len(sorted_nonidle_list_instances_only)
        if not total_found == count:
            LOG.info("OO can't supply enough (%d) instances for termination. Found only %d", count, total_found)

        return idle_list, sorted_nonidle_list_instances_only

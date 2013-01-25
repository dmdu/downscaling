import logging
import operator
import time

from threading import Thread
from resources.clouds import Cloud
from lib.util import RemoteCommand
from resources.jobs import Jobs
from lib.logger import filelog


LOG = logging.getLogger(__name__)

class Replacer(Thread):

    def __init__(self, stop_event, config, master, interval=15):
        Thread.__init__(self)
        self.master = master
        self.interval = interval
        self.config = config
        self.stop_event = stop_event
        self.determine_desired()

    def determine_desired(self):
        # assigns both the total count and the desired dict (by cloud)
        self.desired_total = 0
        self.desired_dict = {}
        for group in self.config.workers.worker_groups:
            count = int(group['desired'])
            self.desired_total += count
            self.desired_dict[group['cloud']] = count
        LOG.info("Replacer determined desired total: %d" % (self.desired_total))
        LOG.info("Replacer determined desired dictionary: %s" % (str(self.desired_dict)))
        return

    def get_current_instance_count(self):
        count = 0
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            count += len(cloud.get_instances())

        # we only care about worker instances here, so don't count the master
        count -= 1

        LOG.info("Replacer found current instance count: %d" % (count))
        return count

    def get_current_instance_dict(self):

        pool_dict = {}
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            # we only care about worker instances here, so don't count the master
            count = len(cloud.get_instances(exclude_dns=self.master.dns))
            pool_dict[acloud] = count

        LOG.info("Replacer found current instance dictionary: %s" % (str(pool_dict)))

        # Worker pool logging
        pool_dict_str = "%s," % (time.time())
        for cloud_name, instance_count in pool_dict.iteritems():
            pool_dict_str += "%s:%d," % (cloud_name,instance_count)
        # remove last char
        pool_dict_str = pool_dict_str[:-1]
        filelog(self.config.worker_pool_log, pool_dict_str)

        return pool_dict

    def get_sorted_candidate_list(self):

        curr_dict = self.get_current_instance_dict()

        diff_dict = {}
        for cloud_name in self.desired_dict.keys():
            diff = self.desired_dict[cloud_name] - curr_dict[cloud_name]
            if diff >= 0:
                diff_dict[cloud_name] = diff

        sorted_dict = sorted(diff_dict.iteritems(), key=operator.itemgetter(1))
        sorted_dict.reverse()

        LOG.info("Replacer found candidate clouds for adding missing instances: %s" % str(sorted_dict))

        clouds_list = []
        for atyple in sorted_dict:
            clouds_list.append(Cloud(atyple[0], self.config))
        return clouds_list


    def get_missing_count(self):
        current_count = self.get_clouds_instances_count()
        return  self.desired_total - current_count

    def replace_failed_instance(self):

        current_count = self.get_current_instance_count()
        missing_count = self.desired_total - current_count
        clouds_list = self.get_sorted_candidate_list()

        if missing_count > 0:
            LOG.info("Replacer is missing %d instances" % (missing_count))
            for acloud in clouds_list:
                for group in self.config.workers.worker_groups:
                    if group['cloud'] == acloud.name:
                        break
                    else:
                        continue

                try:
                    LOG.info("Replacer is adding %d instances to %s" % (missing_count, acloud.name))
                    boot_result = acloud.boot_image(
                        group['image_id'], count=missing_count, type= group['instance_type'], user_data = self.master.dns)

                    for instance in boot_result.instances:
                        LOG.info("Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s) added"
                                 % (acloud.name, boot_result.id, instance.id, instance.public_dns_name))
                        filelog(self.config.node_log, "ADDED WORKER cloud: %s, instance: %s, dns: %s" %
                                                  (acloud.name, instance.id, instance.public_dns_name))

                    if boot_result:
                        LOG.info("Boot results of instances is %s" % (boot_result))
                        break
                except Exception as exp:
                    print str(exp)
        else:
            LOG.info("Replacer is not missing any instances. Sleeping")


#    def get_running_jobs(self):
#
#        command = "condor_q -run | grep %s" % (self.config.workload.user)
#        rcmd = RemoteCommand(
#            config = self.config,
#            hostname = self.master.dns,
#            ssh_private_key = self.config.globals.priv_path,
#            user = self.config.workload.user,
#            command = command)
#        rcmd.execute()
#        jobs = Jobs(rcmd.stdout, self.config.workload.user)
#        return jobs


    def run(self):
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)
            self.replace_failed_instance()

            # No need for this: when workload thread stops, this thread will be stopped
            #jobs = self.get_running_jobs()
            #if not jobs.list:
            #    LOG.info("No jobs in the queue. Terminating Replacer")
            #    self.stop_event.set()
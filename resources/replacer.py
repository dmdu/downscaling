import logging
import operator

from threading import Thread
from resources.clouds import Cloud
from lib.util import RemoteCommand
from resources.jobs import Jobs


LOG = logging.getLogger(__name__)

class Replacer(Thread):

    def __init__(self, stop_event, config, master, interval=15):
        Thread.__init__(self)
        self.master = master
        self.interval = interval
        self.config = config
        self.stop_event = stop_event

    def get_clouds_instances_count(self):
        count = 0
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            count = count + len(cloud.get_instances())
        count = count -1
        LOG.info("we have %d total instances right now" % (count))
        return count

    def get_initial_clouds_instances_count(self):
        count = 0
        for group in self.config.workers.worker_groups:
            count = int(group['initial']) + count
        LOG.info("we had %d total instances initially" % (count))
        return count

    def get_cloud_sorted_by_desired_ratio(self):
        desired_dict = {}

        for group in self.config.workers.worker_groups:
            desired_dict[group['cloud']] = int(group['desired'])

        initial_dict = {}

        for group in self.config.workers.worker_groups:
            initial_dict[group['cloud']] = int(group['initial'])

        diff_dict = {}
        for cloud_name in desired_dict.keys():
            diff = desired_dict[cloud_name] - initial_dict[cloud_name]
            if diff >= 0:
                diff_dict[cloud_name] = diff

        sorted_dict = sorted(diff_dict.iteritems(), key=operator.itemgetter(1))
        sorted_dict.reverse()

        LOG.info("cloud sorted by desired ration %s" % str(sorted_dict) )

        clouds_list = []
        for atyple in sorted_dict:
            clouds_list.append(Cloud(atyple[0], self.config))

        return clouds_list


    def get_missing_count(self):
        current_count = self.get_clouds_instances_count()
        initial_count = self.get_initial_clouds_instances_count()
        return initial_count - current_count


    def replace_failed_instance(self):
        missing_count = self.get_missing_count()
        clouds_list = self.get_cloud_sorted_by_desired_ratio()

        if missing_count > 0:
            LOG.info("we are missing %d instances" % (missing_count))
            for acloud in clouds_list:
                for group in self.config.workers.worker_groups:
                    if group['cloud'] == acloud.name:
                        break
                    else:
                        continue

                try:
                    LOG.info("adding %d instances to %s" % (missing_count, acloud.name))
                    boot_result = acloud.boot_image( group['image_id'], count=missing_count, type= group['instance_type'], user_data = self.master.dns)
                    if boot_result:
                        LOG.info("Boot results of instances is %s" % (boot_result))
                        break
                except Exception as exp:
                    print str(exp)
        else:
            LOG.info("we are not missing any instances, continue to wait")


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


    def run(self):
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)
            self.replace_failed_instance()

            jobs = self.get_running_jobs()
            if not jobs.list:
                LOG.info("No jobs in the queue. Terminating Replacer")
                self.stop_event.set()
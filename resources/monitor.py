import logging
from threading import Thread
import time
import datetime
import copy

from lib.util import RemoteCommand
from resources.jobs import Job, Jobs
from lib.logger import filelog
from resources.workerpool import WorkerPool
from resources.workers import Worker, Workers

LOG = logging.getLogger(__name__)

class AdditionalWorker(Thread):

    def __init__(self, config, cloud, master_dns, sleep_period_sec=5):

        Thread.__init__(self)
        self.config = config
        self.cloud_name = cloud.name
        self.cloud = cloud
        self.master_dns = master_dns
        self.sleep_period_sec = float(sleep_period_sec)
        self.new_worker = None

    def run(self):

        LOG.info("Additional Worker thread: adding a worker in: %s" % (self.cloud_name))

        group = None
        for group in self.config.workers.worker_groups:
            if group['cloud'] == self.cloud_name:
                break
        if not group:
            LOG.error("Can't find worker group for cloud name: %s" % (self.cloud_name))
            return

        reservation = self.cloud.boot_image(image_id=group['image_id'], count=1, type=group['instance_type'])
        timestamp = datetime.datetime.now()
        instances = reservation.instances
        instance = instances[0]
        worker = Worker(self.config, self.cloud, reservation, instance, timestamp)
        LOG.info(
            "Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added"
            % (worker.cloud.name, worker.reservation_id, worker.instance_id, worker.dns, worker.launch_time))
        filelog(self.config.node_log, "ADDED WORKER cloud: %s, reservation: %s, instance: %s, dns: %s"
                                      % (worker.cloud.name, worker.reservation_id, worker.instance_id, worker.dns))

        # Sleep until running
        running = False
        while not running:
            all_reservations = self.cloud.conn.get_all_instances()
            for checked_reservation in all_reservations:
                if checked_reservation.id == worker.reservation_id and (not running):
                    for checked_instance in checked_reservation.instances:
                        if checked_instance.id == worker.instance_id and (not running):
                            if checked_instance.state == "running":
                                LOG.info("Additional worker instance \"%s\" of reservation \"%s\" in cloud \"%s\" is running" %
                                     (checked_instance.id, checked_reservation.id, self.cloud.name))
                                running = True
                            else:
                                time.sleep(self.sleep_period_sec)
                                LOG.info("Additional worker instance \"%s\" is not running yet" % (checked_instance.id))

        # Contextualize
        rc = RemoteCommand(
            config = self.config,
            hostname = worker.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = 'root',
            command = "%s %s" % (group['script_path'], self.master_dns))
        code = rc.execute()
        print rc.stderr
        print rc.stdout
        if code == 0:
            LOG.info("Additional worker node \"%s\" was contextualized successfully. Details are in remote log file"
                     % (worker.instance_id))
        else:
            LOG.error("Error occurred during contextualization of additional worker node \"%s\""
                      % (worker.instance_id))

        LOG.info("Additional worker instance \"%s\" is ready to be returned and added to the pool" % (worker.instance_id))
        self.new_worker = worker

class Monitor(Thread):

    def __init__(self, config, clouds, master, workers, interval=10):

        Thread.__init__(self)
        self.config = config
        self.clouds = clouds
        self.master = master
        self.interval = interval
        self.workers = workers
        self.prev_pool = None
        self.additionalworkers = list()
        self.cloud_blacklist = list() # cloud is added to this list if there is a thread adding a worker in it

        # Get desired work pool
        desired_dict = {}
        for group in self.config.workers.worker_groups:
            desired_dict[group['cloud']] = int(group['desired'])
        self.desired_pool = WorkerPool(desired_dict)
        LOG.info("Monitor obtained desired worker pool info: %s" % (self.desired_pool.get_str()))

    def run(self):

        LOG.info("Activating Monitor. Sleep period: %d sec" % (self.interval))
        while True:
            time.sleep(self.interval)

            jobs = self.get_running_jobs()

            curr_dict, curr_dict_str = self.match_workers_to_cloud()
            print curr_dict_str
            filelog(self.config.worker_pool_log, curr_dict_str)
            curr_pool = WorkerPool(curr_dict)

            failures = curr_pool.detect_changes(self.prev_pool)
            self.prev_pool = copy.copy(curr_pool)

            print "Failures: %s" % (str(failures))

            self.actuate(curr_pool, failures, jobs)

            # check additional worker threads
            if self.additionalworkers:
                LOG.info("Monitor detected %d additional worker thread(s)" % len(self.additionalworkers))
                for worker_thread in self.additionalworkers:
                    if not worker_thread.isAlive(): # run has finished
                        LOG.info("Detected finished tread: %s" % str(worker_thread))

                        #if worker_thread.new_worker:

                        LOG.info("Tread has a new_worker: id: %s, launch_time: %s"
                                 % (worker_thread.new_worker.instance_id, worker_thread.new_worker.launch_time))
                        self.workers.list.append(worker_thread.new_worker)
                        self.workers.form_cloud_to_lists()
                        self.additionalworkers.remove(worker_thread)
                        LOG.info("Monitor added worker node %s to the workers list"
                                 % worker_thread.new_worker.instance_id)

                        # allow to add workers in this cloud from now on again
                        if worker_thread.new_worker.cloud.name in self.cloud_blacklist:
                            self.cloud_blacklist.remove(worker_thread.new_worker.cloud.name)
                            LOG.info("Monitor unblocked cloud %s from launching new workers"
                                     % (worker_thread.new_worker.cloud.name))

            # Keep the monitor running for now

            #if len(jobs.list) == 0:
            #    LOG.info("No jobs in the queue. Terminating Monitor")
            #    break

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

    def query_current_workers(self):
        """ Connect to master, get list of all workers hostname and return it"""

        workers_dns_list = []

        command = "condor_status"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = command)
        rcmd.execute()

        if rcmd.stdout:
            # condor status will be lines so split them
            all_lines = rcmd.stdout.split("\n")
            for line in all_lines:
                # line not empty
                if line.strip():
                # if its the first line then go to the next one
                    if line.strip().startswith("Name"):
                        continue
                    # if we find a line that starts with total then we are done, break out from the loop
                    elif line.strip().startswith("Total"):
                        break
                    # it must be a line of interest, parse it
                    else:
                        # split line by space :
                        #"vm-148-102.uc.futu LINUX      X86_64 Unclaimed Idle     0.150  2048  0+00:00:04"
                        line_columns = line.split()
                        try:
                            tmp_fqdn = line_columns[0].strip()
                            workers_dns_list.append(tmp_fqdn)
                        except Exception as expt:
                            LOG.info("Error parsing condor status, line says : %s and the expt says : %s" % (line, str(expt)))

        return workers_dns_list

#    def match_workers_to_cloud(self):
#        """
#        I found out that hotels hostname always looks like vm-148-103.uc.futuregrid.org and sierra looks like
#        vm-9.sdsc.futuregrid.org. Also condor status always return at least first and second parts of the hostname,
#        "vm-148-102.uc.futu" so I used this information to determine which cloud a vm belong to.
#        """
#        current_workers = self.query_current_workers()
#
#        clouds_dict = {}
#        for acloud in self.config.clouds.list:
#            clouds_dict[acloud] = 0
#
#        for vms_fqdn in current_workers:
#            try:
#                fqdn_parts = vms_fqdn.split(".")
#                if fqdn_parts[1] == "uc":
#                    clouds_dict["hotel"] += 1
#                elif fqdn_parts[1] == "sdsc":
#                    clouds_dict["sierra"] += 1
#                else:
#                    LOG.info("Got strange hostname from condor status, line says : %s" % (vms_fqdn))
#            except Exception as expt:
#                LOG.info("Error parsing condor status, line says : %s and the expt says : %s" % (vms_fqdn, str(expt)))
#
#        return {time.time():clouds_dict}


    def match_workers_to_cloud(self):
        """
        This version of the same functions depends on provided workers object and rely on workers.cloud_to_instance_dns_list['hotel']
        """

        current_workers = self.query_current_workers()
        current_workers_two_parts = [ ".".join(x.split(".")[:2]) for x in current_workers]
        clouds_dict = {}

        for acloud in self.config.clouds.list:
            clouds_dict[acloud] = 0
            vms_dns = self.workers.cloud_to_instance_dns_list[acloud]
            vms_dns_two_parts = [ ".".join(x.split(".")[:2]) for x in vms_dns]

            for worker in current_workers_two_parts:
                if worker in vms_dns_two_parts:
                    clouds_dict[acloud] += 1

        timestamp = time.time()
        str_format = "%s," % (timestamp)
        for cloud_name, instance_count in clouds_dict.iteritems():
            str_format += "%s:%s," % (cloud_name,str(instance_count))
        # remove last char
        str_format = str_format[:-1]
        return clouds_dict, str_format

    def actuate(self, curr_pool, failures, jobs):

        LOG.info("Actuate method has been called")
        if failures:

            # pick the cloud with the maximum number of failures
            # if there are several candidates, pick any
            #sorted_failures = list(sorted(failures, key=failures.__getitem__, reverse=True))
            # there must be at least one failure at this point
            #failure_cloud = sorted_failures[0]
            #failure_number = failures[failure_cloud]
            scores = {}
            for group in self.config.workers.worker_groups:
                cloud_name = group['cloud']
                potential_pool = curr_pool.pool_with_additional_worker(cloud_name)
                distance = self.desired_pool.ratio_distance(potential_pool)
                scores[cloud_name] = distance
            LOG.info("Monitor calculated scores for adding a worker: %s" % str(scores))

            # lowest distance(score) wins
            ranked_clouds = list(sorted(scores, key=scores.__getitem__, reverse=False))
            LOG.info("Monitor ranked clouds for adding a worker: %s" % str(ranked_clouds))

            winner_cloud_name = ranked_clouds[0]
            LOG.info("Monitor determined the winner cloud: %s" % winner_cloud_name)
            winner_cloud = self.clouds.lookup_by_name(winner_cloud_name)

            if not winner_cloud_name in self.cloud_blacklist:
                self.cloud_blacklist.append(winner_cloud_name)
                worker_thread = AdditionalWorker(self.config, winner_cloud, self.master.dns)
                worker_thread.start()
                self.additionalworkers.append(worker_thread)
            else:
                LOG.info("Monitor ignored detected failure since a worker has been added to the cloud %s already"
                         % (winner_cloud_name))
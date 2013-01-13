import logging
import sys
import time
import datetime

from lib.util import RemoteCommand
from lib.util import is_yes, printfile
from lib.logger import filelog

LOG = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, config, cloud, reservation, instance, timestamp):

        self.config = config
        self.cloud = cloud
        self.reservation_id = reservation.id
        self.instance_id = instance.id
        self.dns = instance.public_dns_name
        self.launch_time = timestamp

    def terminate(self):

        command = "/etc/init.d/condor stop"
        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = 'root',
            command = command)
        code = rcmd.execute()
        if code == 0:
            LOG.info("Successfully stopped Condor daemon on worker instance: %s" % (self.instance_id))
        else:
            LOG.error("Error occurred during Condor daemon termination on worker instance: %s" % (self.instance_id))

        for reservation in self.cloud.conn.get_all_instances():
            #LOG.info("Checking reservation: %s" % (reservation.id))
            if reservation.id == self.reservation_id:
                for instance in reservation.instances:
                    #LOG.info("Checking instance: %s" % (instance.id))
                    if instance.id == self.instance_id:
                        instance.terminate()
                        LOG.info("Terminated instance: %s" % (instance.id))

class Workers(object):

    def __init__(self, config, clouds, master):

        self.config = config
        self.clouds = clouds
        self.master = master
        self.list = list()
        self.cloud_to_instance_id_list = {}
        self.cloud_to_instance_dns_list = {}

        decision_made = False
        create = True
        while decision_made == False:
            input = raw_input( "Create new worker node(s) or reuse existing? (C/R)\n" )
            if input == 'C' or input == 'c' or input == 'Create' or input == 'create':
                create = True
                decision_made = True
            elif input == 'R' or input == 'r' or input == 'Reuse' or input == 'reuse':
                create = False
                decision_made = True
            else:
                print("Invalid input. Please try again.\n")

        if create:

            LOG.info("Worker nodes are going to be created")
            for group in self.config.workers.worker_groups:
                cloud = self.clouds.lookup_by_name(group['cloud'])
                if cloud == None:
                    LOG.error("Cloud \"%s\" cannot be found in the clouds config file" % (group['cloud']))
                    sys.exit(1)

                #wg = WorkerGroup(self.config, group, cloud, master)

                initial = int(group['initial'])
                LOG.info("Launching %d worker(s) in the cloud %s" % (initial, cloud.name))
                if initial > 0:
                    reservation = cloud.boot_image(
                        image_id=group['image_id'], count=initial, type=group['instance_type'])
                    timestamp = datetime.datetime.now()
                    for instance in reservation.instances:
                        worker = Worker(self.config, cloud, reservation, instance, timestamp)
                        self.list.append(worker)
                        LOG.info(
                            "Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added to the list"
                            % (worker.cloud.name, worker.reservation_id, worker.instance_id,
                               worker.dns, worker.launch_time))
                        filelog(self.config.node_log, "CREATED WORKER cloud: %s, reservation: %s, instance: %s, dns: %s" %
                                                      (worker.cloud.name, worker.reservation_id, worker.instance_id, worker.dns))

        else:
            # Reusing existing worker nodes

            LOG.info("Worker nodes are going to be reused")
            #printfile(self.config.node_log)

            for group in self.config.workers.worker_groups:
                cloud = self.clouds.lookup_by_name(group['cloud'])
                if cloud == None:
                    LOG.error("Cloud \"%s\" cannot be found in the clouds config file" % (group['cloud']))
                    sys.exit(1)
                initial = int(group['initial'])
                LOG.info("Need to have %d worker(s) in the cloud %s initially" % (initial, cloud.name))

                count = 0
                enough = (count == initial)
                if not enough:
                    if cloud.conn == None:
                        cloud.connect()
                    for reservation in cloud.conn.get_all_instances():
                        if not enough:
                            for instance in reservation.instances:
                                if not enough:
                                    printfile(self.config.node_log, "Log entries for instance %s:" % instance.id, instance.id)
                                    select_instance = raw_input(
                                        "Select instance \"%s\" of reservation \"%s\" in cloud \"%s\" as a worker node? (Y/N)\n"
                                        % (instance.id, reservation.id, cloud.name))
                                    if is_yes(select_instance):
                                        LOG.info("Instance \"%s\" of reservation \"%s\" in cloud \"%s\" has been selected as a worker node"
                                                % (instance.id, reservation.id, cloud.name))
                                        timestamp = datetime.datetime.now()
                                        worker = Worker(self.config, cloud, reservation, instance, timestamp)
                                        self.list.append(worker)
                                        LOG.info(
                                            "Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added to the list"
                                            % (worker.cloud.name, worker.reservation_id, worker.instance_id,
                                            worker.dns, worker.launch_time))
                                        filelog(self.config.node_log, "REUSED WORKER cloud: %s, reservation: %s, instance: %s, dns: %s" %
                                                                      (worker.cloud.name, worker.reservation_id, worker.instance_id, worker.dns))
                                        count += 1
                                        enough = (count == initial)
                                else:
                                    break
                        else:
                            break
                LOG.info("Selected %d out of %d worker(s) in the cloud %s" % (count, initial, cloud.name))

                # There might be not enough instances running already
                if not enough:
                    difference = initial - count
                    LOG.info("In addition to selected workers, launching %d worker(s) in the cloud %s" % (difference, cloud.name))
                    reservation = cloud.boot_image(image_id=group['image_id'], count=difference, type=group['instance_type'])
                    timestamp = datetime.datetime.now()
                    for instance in reservation.instances:
                        worker = Worker(self.config, cloud, reservation, instance, timestamp)
                        self.list.append(worker)
                        LOG.info(
                            "Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added to the list"
                            % (worker.cloud.name, worker.reservation_id, worker.instance_id,
                            worker.dns, worker.launch_time))
                        filelog(self.config.node_log, "CREATED WORKER cloud: %s, reservation: %s, instance: %s, dns: %s" %
                                                      (worker.cloud.name, worker.reservation_id, worker.instance_id, worker.dns))
                        #count += difference
                        #enough = (count == initial)

        self.form_cloud_to_lists()
        #print self.cloud_to_instance_id_list
        #print self.cloud_to_instance_dns_list

        self.sleep_until_all_workers_ready()
        self.contextualize()

    def form_cloud_to_lists(self):

        for cloud in self.clouds.list:
            self.cloud_to_instance_id_list[cloud.name] = list()
            self.cloud_to_instance_dns_list[cloud.name] = list()
        for worker in self.list:
            # append to the lists
            (self.cloud_to_instance_id_list[worker.cloud.name]).append("%s" % worker.instance_id)
            (self.cloud_to_instance_dns_list[worker.cloud.name]).append("%s" % worker.dns)

    def sleep_until_all_workers_ready(self, sleep_period_sec=20):

        LOG.info('Waiting until all workers are running')

        for cloud in self.clouds.list:
            worker_instances = self.cloud_to_instance_id_list[cloud.name]
            if worker_instances:    # list is not empty
                while True:

                    #print worker_instances
                    all_reservations = cloud.conn.get_all_instances()
                    for reservation in all_reservations:
                        for instance in reservation.instances:
                            #print "Checking instance %s" % (instance.id)
                            if instance.id in worker_instances:
                                #print "Instance %s is in the list, state: %s" % (instance.id, instance.state)
                                if instance.state == "running":
                                    LOG.info("Worker instance \"%s\" of reservation \"%s\" in cloud \"%s\" is running" %
                                            (instance.id, reservation.id, cloud.name))
                                    worker_instances.remove(instance.id)
                    if not worker_instances:    # list is empty
                        LOG.info('All workers in cloud %s are running now' % (cloud.name))
                        break
                    LOG.info("Workers aren't ready yet. Sleeping")
                    time.sleep(sleep_period_sec)
            else: # list is empty
                LOG.info('No workers in cloud %s to wait for' % (cloud.name))

        LOG.info('All workers are running now')


    def contextualize(self):

        script_paths = {}

        for group in self.config.workers.worker_groups:
            script_paths[group['cloud']] = group['script_path']

        for worker in self.list:

            if not worker.cloud.name in script_paths:
                LOG.error("Path to the script for worker contextualization can't be found")
                sys.exit(1)

            script_path = script_paths[worker.cloud.name]

            rc = RemoteCommand(
                config = self.config,
                hostname = worker.dns,
                ssh_private_key = self.config.globals.priv_path,
                user = 'root',
                command = "%s %s" % (script_path, self.master.dns))

            code = rc.execute()
            if code == 0:
                LOG.info("Worker node \"%s\" was contextualized successfully. Details are in remote log file"
                         % (worker.instance_id))
            else:
                LOG.error("Error occurred during contextualization of worker node \"%s\""
                          % (worker.instance_id))

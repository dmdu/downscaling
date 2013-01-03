import logging
import sys
import time
import datetime

from lib.util import RemoteCommand

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

class WorkerGroup(object):

    def __init__(self, config, group, cloud, master):

        self.config = config
        self.group = group
        self.cloud = cloud
        self.master = master
        initial = int(self.group['initial'])
        LOG.info("%d worker node(s) is(are) going to be created in the cloud: %s" % (initial, self.cloud.name))
        self.group_list = list()
        if initial == 0:
            self.reservation = None
        else:
            self.reservation = self.cloud.boot_image(
                image_id=self.group['image_id'], count=initial, type=self.group['instance_type'])
            timestamp = datetime.datetime.now()
            for instance in self.reservation.instances:
                self.group_list.append(Worker(config, self.cloud, self.reservation, instance, timestamp))

class Workers(object):

    def __init__(self, config, clouds, master):

        self.config = config
        self.master = master
        self.list = list()
        self.groups = list()
        self.count = 0
        for group in config.workers.worker_groups:
            cloud = clouds.lookup_by_name(group['cloud'])
            if cloud == None:
                LOG.error('Cloud \"%s\" cannot be found in the clouds config file' % (group['cloud']))
                sys.exit(1)
            wg = WorkerGroup(config, group, cloud, master)
            if wg.reservation != None:
                self.groups.append(wg)
                for worker in wg.group_list:
                    self.list.append(worker)
                    self.count += 1
                    LOG.info(
                        'Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added to the list'
                             % (worker.cloud.name, worker.reservation_id, worker.instance_id,
                                worker.dns, worker.launch_time))

        self.sleep_until_all_workers_ready()
        self.contextualize()

    def sleep_until_all_workers_ready(self, sleep_period_sec=5):

        LOG.info('Waiting until all workers are running')
        while not self.are_all_workers_ready():
            time.sleep(sleep_period_sec)
        LOG.info('All workers are running now')

    def are_all_workers_ready(self):

        for wg in self.groups:
            all_reservations = wg.cloud.conn.get_all_instances()
            worker_reservation = wg.reservation
            for reservation in all_reservations:
                if reservation.id == worker_reservation.id:
                    for instance in reservation.instances:
                        if instance.state == "running":
                            LOG.info("Worker instance \"%s\" of reservation \"%s\" in cloud \"%s\" is running" %
                                      (instance.id, reservation.id, wg.cloud.name))
                        else:
                            return False
        return True

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

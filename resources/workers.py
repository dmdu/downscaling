import logging
import sys
import time
import datetime

from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, cloud, reservation, instance, timestamp):

        self.cloud = cloud
        self.reseration_id = reservation.id
        self.instance_id = instance.id
        self.dns = instance.public_dns_name
        self.launch_time = timestamp

class WorkerGroup(object):

    def __init__(self, group, cloud, master):

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
                self.group_list.append(Worker(self.cloud, self.reservation, instance, timestamp))

class Workers(object):

    def __init__(self, config, clouds, master):

        self.list = list()
        for group in config.workers.worker_groups:
            cloud = clouds.lookup_by_name(group['cloud'])
            if cloud == None:
                LOG.error('Cloud \"%s\" cannot be found in the clouds config file' % (group['cloud']))
                sys.exit(1)
            wg = WorkerGroup(group, cloud, master)
            for worker in wg.group_list:
                self.list.append(worker)
                LOG.info(
                    'Worker (Cloud: %s, Reservation: %s, Instance: %s, DNS: %s, LaunchTime: %s) added to the list'
                         % (worker.cloud.name, worker.reseration_id, worker.instance_id,
                            worker.dns, worker.launch_time))

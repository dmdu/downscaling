import logging
import sys
import datetime

from lib.util import RemoteCommand
from lib.logger import filelog

LOG = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, config, instance):

        self.config = config
        self.instance = instance
        self.dns = instance.public_dns_name

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
            LOG.info("Successfully stopped Condor daemon on worker instance: %s" % (self.instance.id))
        else:
            LOG.error("Error occurred during Condor daemon termination on worker instance: %s" % (self.instance.id))

        LOG.info("Terminating instance: %s, %s" % (self.instance.id, self.dns))
        self.instance.terminate()

class Workers(object):

    def __init__(self, config, clouds, master):

        self.config = config
        self.clouds = clouds
        self.master = master
        self.total_number = 0

    def create(self):

        LOG.info("Worker nodes are going to be created")
        for group in self.config.workers.worker_groups:
            cloud = self.clouds.lookup_by_name(group['cloud'])
            if cloud == None:
                LOG.error("Cloud \"%s\" cannot be found in the clouds config file" % (group['cloud']))
                sys.exit(1)

            initial = int(group['initial'])
            LOG.info("Launching %d worker(s) in the cloud %s" % (initial, cloud.name))
            if initial > 0:
                reservation = cloud.boot_image(
                    image_id=group['image_id'], count=initial, type=group['instance_type'], user_data=self.master.dns)
                self.total_number += initial

                for instance in reservation.instances:
                    worker = Worker(self.config, instance)
                    LOG.info(
                        "Worker (Cloud: %s, Instance: %s, DNS: %s) added to the list"
                        % (cloud.name, worker.instance.id, worker.dns))
                    filelog(self.config.node_log, "CREATED WORKER cloud: %s, instance: %s, dns: %s" %
                                                  (cloud.name, worker.instance.id, worker.dns))

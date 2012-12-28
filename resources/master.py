import logging
import sys
import time

from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class Master(object):
    """
    """
    def __init__(self, config, clouds):
        self.config = config
        self.cloud = clouds.lookup_by_name(config.master.cloud)
        if self.cloud == None:
            LOG.error("Can't find a cloud \"%s\" specified for the master node" % (config.master.cloud))
            sys.exit(1)
        LOG.info("Master node is going to be created in the cloud: %s" % (config.master.cloud))
        self.reservation = self.cloud.boot_image(config.master.image_id, count=1, type=config.master.instance_type)
        self.sleep_until_master_ready()
        self.determine_dns()
        self.contextualize()

    def sleep_until_master_ready(self, sleep_period_sec=5):

        LOG.info("Waiting until master node is running")
        while not self.cloud.is_reservation_ready(self.reservation):
            time.sleep(sleep_period_sec)
        LOG.info("Master reservation is running now")

    def determine_dns(self):

        instances = self.reservation.instances
        if len(instances) == 1:
            self.dns = (instances[0]).public_dns_name
            LOG.info("Determined master node's public DNS name: %s" % (self.dns))
        else:
            LOG.error("There should not be more than 1 master node")

    def terminate(self):

        for instance in self.reservation.instances:
            instance.terminate()
            LOG.info("Terminated instance: " + instance.id)

    def contextualize(self):

        rc = RemoteCommand(
            config = self.config,
            hostname = self.dns,
            ssh_private_key = self.config.globals.priv_path,
            user = 'root',
            command = self.config.master.script_path)

        code = rc.execute()
        if code == 0:
            LOG.info("Master node was contextualized successfully. Details are in remote log file")
        else:
            LOG.error("Error occurred during master node's contextualization")
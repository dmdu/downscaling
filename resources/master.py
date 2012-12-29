import logging
import sys
import time

from lib.util import RemoteCommand
from lib.util import is_yes

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

        decision_made = False
        create = True
        while decision_made == False:
            input = raw_input( "Create a new master node or reuse existing? (C/R)\n" )
            if input == 'C' or input == 'c' or input == 'Create' or input == 'create':
                create = True
                decision_made = True
            elif input == 'R' or input == 'r' or input == 'Reuse' or input == 'reuse':
                create = False
                decision_made = True
            else:
                print("Invalid input. Please try again.\n")

        if create:
            LOG.info("Master node is going to be created in the cloud: %s" % (config.master.cloud))
            self.reservation = self.cloud.boot_image(config.master.image_id, count=1, type=config.master.instance_type)
            self.sleep_until_master_ready()
            self.determine_dns()
            self.contextualize()
        else:
            LOG.info("One of the existing instances in cloud \"%s\" is going to be reused as a master node"
                     % (self.cloud.name))
            self.cloud.connect()
            master_selected = False
            while master_selected == False:

                for reservation in self.cloud.conn.get_all_instances():
                    instances = reservation.instances
                    if len(instances) != 1:
                        LOG.info("Skipping reservation \"%s\" since it has more than one instance" % (reservation.id))
                        continue
                    instance = instances[0]
                    select_instance = raw_input(
                        "Select instance \"%s\" of reservation \"%s\" in cloud \"%s\" as a master node? (Y/N)\n"
                        % (instance.id, reservation.id, self.cloud.name))
                    if is_yes(select_instance):
                        LOG.info("Master node has been selected. Instance: %s, Reservation: %s, Cloud: %s"
                                % (instance.id, reservation.id, self.cloud.name))
                        master_selected = True
                        self.reservation = reservation
                        self.determine_dns()
                        self.contextualize()
                        break
                if master_selected == False:
                    print("Master node has not been selected. Looping through the list of existing reservations again.")

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
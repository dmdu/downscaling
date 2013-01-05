import logging
import random
import os


from threading import Thread
from resources.clouds import Cloud

LOG = logging.getLogger(__name__)

class FailureSimulator(Thread):

    def __init__(self, stop_event, config, master, interval=40):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.master = master
        self.interval = interval
        random.seed(os.urandom(128))

    def get_termination_list(self):
        list_of_clouds = []
        for acloud in self.config.clouds.list:
            cloud = Cloud(acloud, self.config)
            list_of_clouds.append(cloud)

        # figure out how many vms we have in all our clouds
        list_of_vms = []
        for acloud in list_of_clouds:
            vms = acloud.get_instances()
            if vms:
                list_of_vms.extend(vms)

        # find the master, remove it from the list and break out of the loop :
        for avm_index, avm_instance in enumerate(list_of_vms):
            if avm_instance.public_dns_name == self.master.dns:
                del list_of_vms[avm_index]
                break

        return list_of_vms

    def run(self):

        LOG.info("Activating Failure Simulator. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            #time.sleep(self.interval)
            self.stop_event.wait(self.interval)

            list_of_vms = self.get_termination_list()
            # continue as normal
            count = len(list_of_vms)
            if count > 0:
                pick = random.randint(0, count-1)
                instance = list_of_vms[pick]
                instance.terminate()
                LOG.info("Failure Simulator terminated an instance %s (%s)" % (instance.id, instance.public_dns_name))
            else:
                LOG.info("No instances to kill. Terminating Failure Simulator")
                self.stop_event.set()


class ExpFailureSimulator(FailureSimulator):
    """
    terminate one VM at a time using exponential failure distribution.
    """

    def __init__(self, stop_event, config, master, interval=240):
        FailureSimulator.__init__(stop_event, config, master, interval)

    def run(self):

        LOG.info("Activating Failure Simulator. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)

            list_of_vms = self.get_termination_list()

            # continue as normal
            count = len(list_of_vms)
            if count > 0:
                pick = random.randint(0, count-1)
                instance = list_of_vms[pick]
                instance.terminate()
                LOG.info("Failure Simulator terminated an instance %s (%s)" % (instance.id, instance.public_dns_name))
                self.interval = random.expovariate(self.config.failuresimulator.failure_rate) + int(self.config.failuresimulator.min_interval)
            else:
                LOG.info("No instances to kill. Terminating Failure Simulator")
                self.stop_event.set()
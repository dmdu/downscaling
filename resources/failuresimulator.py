import logging
import random
from threading import Thread

LOG = logging.getLogger(__name__)

class FailureSimulator(Thread):

    def __init__(self, stop_event, config, workers, interval=240):

        Thread.__init__(self)
        self.stop_event = stop_event
        self.config = config
        self.workers = workers
        self.interval = interval

    def run(self):

        LOG.info("Activating Failure Simulator. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            #time.sleep(self.interval)
            self.stop_event.wait(self.interval)

            count = len(self.workers.list)
            if count > 0:
                pick = random.randint(0, count-1)
                worker = self.workers.list[pick]
                worker.terminate()
                del self.workers.list[pick]
                LOG.info("Failure Simulator terminated an instance %s (%s)" % (worker.instance_id, worker.dns))
            else:
                LOG.info("No instances to kill. Terminating Failure Simulator")
                self.stop_event.set()


class ExpFailureSimulator(FailureSimulator):
    """
    terminate one VM at a time using exponential failure distribution.
    """

    def __init__(self, stop_event, config, workers, interval=240):
        FailureSimulator.__init__(stop_event, config, workers, interval)

    def run(self):

        LOG.info("Activating Failure Simulator. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            self.stop_event.wait(self.interval)

            count = len(self.workers.list)
            if count > 0:
                pick = random.randint(0, count-1)
                worker = self.workers.list[pick]
                worker.terminate()
                del self.workers.list[pick]
                LOG.info("Failure Simulator terminated an instance %s (%s)" % (worker.instance_id, worker.dns))
                self.interval = random.expovariate(self.config.failuresimulator.failure_rate) + int(self.config.failuresimulator.min_interval)

            else:
                LOG.info("No instances to kill. Terminating Failure Simulator")
                self.stop_event.set()


class ExpVmFailureSimulator(FailureSimulator):
    """
    terminate multiple VM on a fixed interval
    """

    def __init__(self, stop_event, config, workers, interval=240):
        FailureSimulator.__init__(stop_event, config, workers, interval)
        self.loops_count = 0

    def run(self):
        LOG.info("Activating Exponential Failure Simulator. Sleep period: %d sec" % (self.interval))
        while(not self.stop_event.is_set()):
            self.loops_count += 1
            self.stop_event.wait(self.interval)
            workers_count = len(self.workers.list)

            if workers_count > 0:

                # 1, 2, 4, 16 .. n
                how_many_to_pick = 2 ** self.terminated_vm_count
                LOG.info("Failure Simulator is terminating %d  of %d" % (how_many_to_pick, workers_count))

                # if the number of worker is less than the number we want to pick, i.e ( pick 6 out of 3 ), then terminate them all and notify the thread
                if workers_count <= how_many_to_pick:
                    for worker_index, worker in enumerate(self.workers.list):
                        worker.terminate()
                        del self.workers.list[worker_index]
                        LOG.info("Failure Simulator terminated an instance %s (%s)" % (worker.instance_id, worker.dns))
                        self.terminated_vm_count = workers_count
                        self.stop_event.set()

                # or pick a random list, terminate them, and continue the main while loop
                else:
                    random_list = []
                    while len(random_list) != how_many_to_pick:
                        pick = random.randint(0, workers_count-1)
                        if pick not in random_list:
                            random_list.append(pick)

                    for each_index in random_list:
                        worker = self.workers.list[each_index]
                        worker.terminate()
                        del self.workers.list[each_index]
                        LOG.info("Failure Simulator terminated an instance %s (%s)" % (worker.instance_id, worker.dns))
                        self.terminated_vm_count += 1

            else:
                LOG.info("No instances to kill. Terminating Failure Simulator")
                self.stop_event.set()
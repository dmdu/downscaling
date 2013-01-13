import logging
import math
import sys
import time
import datetime
import copy

from lib.util import RemoteCommand
from lib.util import is_yes, printfile
from lib.logger import filelog

LOG = logging.getLogger(__name__)

class WorkerPool(object):

    def __init__(self, dict):
        self.counts = dict
        self.ratios = self.counts_to_ratios(self.counts)

    def counts_to_ratios(self, worker_pool):

        # get total count
        total = 0
        for cloud_name, instance_count in worker_pool.iteritems():
            total += instance_count

        # calculate ratios
        ratios = {}
        if total == 0:
            LOG.info("Worker pool has no workers. Setting ratios to zeros")
            for cloud_name, instance_count in worker_pool.iteritems():
                ratios[cloud_name] = 0
        else:
            for cloud_name, instance_count in worker_pool.iteritems():
                ratios[cloud_name] = float(instance_count)/total

        return ratios

    def get_str(self):

        return "Counts: %s, Ratios: %s" % (str(self.counts), str(self.ratios))

    def detect_changes(self, prev_pool):
        """ Returns a dict like {'sierra': 0, 'hotel': 2} where values are the numbers of failed workers """

        # prev_pool is treated as a worker pool at some moment in the past

        if not prev_pool:
            return None

        failures = {}
        for cloud_name, instance_count in self.counts.iteritems():
            if not (cloud_name in prev_pool.counts):
                LOG.error("Cloud name mismatch. Cloud %s can't be found in the previous worker pool" % (cloud_name))
                continue
            if instance_count > prev_pool.counts[cloud_name]:
                LOG.info("Monitor detected upscaling: %d new worker(s) joined cloud %s"
                         % (instance_count-prev_pool.counts[cloud_name], cloud_name))
            elif instance_count < prev_pool.counts[cloud_name]:
                print "Curr pool: %s" % str(self.counts)
                print "Prev pool: %s" % str(prev_pool.counts)
                diff = prev_pool.counts[cloud_name]-instance_count
                LOG.info("Monitor detected downscaling: only %d worker(s) out of %d is(are) running in cloud %s"
                         % (instance_count, prev_pool.counts[cloud_name], cloud_name))
                failures[cloud_name] = diff
            else:
                LOG.info("Monitor didn't detect any changes in the worker pool in cloud %s" % (cloud_name))
        return failures

    def pool_with_additional_worker(self, cloud):
        """ Returns a pool with an additional worker added in the specified cloud """

        new_counts = copy.copy(self.counts)
        new_counts[cloud] += 1
        return WorkerPool(new_counts)

    def ratio_distance(self, another_pool):
        """ Returns 2-norm of the difference between pool ratios"""

        norm = 0
        for cloud_name, ratio in self.ratios.iteritems():
            if not (cloud_name in another_pool.ratios):
                LOG.error("Name mismatch between pools: %s, %s" % (self.get_str(), another_pool.get_str()))

            diff = ratio - another_pool.ratios[cloud_name]
            norm += math.pow(diff, 2)

        return math.sqrt(norm)








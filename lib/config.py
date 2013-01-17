import logging
import datetime
import os

from lib.util import read_config
from lib.logger import configure_logging

LOG = logging.getLogger(__name__)

class GlobalConfig(object):
    """ GlobalConfig class retrieves information from the file that specifies global parameters """

    def __init__(self, file):
        self.file = file
        self.config = read_config(file)
        default_dict = self.config.defaults()
        self.key_name = default_dict['key_name']
        self.pub_path = default_dict['pub_path']
        self.priv_path = default_dict['priv_path']

class MasterConfig(object):
    """
    """
    def __init__(self, file):
        self.file = file
        self.config = read_config(self.file)
        default_dict = self.config.defaults()
        self.cloud = default_dict['cloud']
        self.instance_type = default_dict['instance_type']
        self.image_id = default_dict['image_id']
        self.script_path = default_dict['script_path']

class CloudsConfig(object):
    """ CloudsConfig class retrieves information from the file that specifies global parameters """

    def __init__(self, file):
        self.file = file
        self.config = read_config(self.file)
        self.list = self.config.sections()

class WorkersConfig(object):

    def __init__(self, file):
        self.file = file
        self.config = read_config(self.file)
        cloud_names = self.config.sections()

        self.worker_groups = list()
        for cloud in cloud_names:
            items = self.config.items(cloud)
            dict = {'cloud': cloud}
            # Form a dictionary out of items list
            for pair in items:
                dict[pair[0]] = pair[1]
            self.worker_groups.append(dict)

class WorkloadConfig(object):

    def __init__(self, file):
        self.file = file
        self.config = read_config(self.file)
        default_dict = self.config.defaults()
        self.user = default_dict['user']
        self.submit_local = default_dict['submit_local']
        self.submit_remote = default_dict['submit_remote']
        self.log_remote = default_dict['log_remote']

class FailureSimulatorConfig(object):

    def __init__(self, afile):
        self.afile = afile
        self.config = read_config(self.afile)
        default_dict = self.config.defaults()
        self.failure_rate = float(default_dict['failure_rate'])
        self.min_interval = float(default_dict['min_interval'])

class PolicyConfig(object):

    def __init__(self, afile):
        self.afile = afile
        self.config = read_config(self.afile)
        default_dict = self.config.defaults()
        self.threshold = default_dict['threshold']
        self.downscaler_interval = int(default_dict['downscaler_interval'])
        self.policy_in_place = default_dict['policy_in_place']

class Config(object):
    """ Config class retrieves all configuration information """

    def __init__(self, options):
        #self.clouds = CloudsConfig(read_config(options.clouds_file))
        #self.benchmarking = BenchmarkingConfig(read_config(options.benchmarking_file))

        self.globals = GlobalConfig(options.global_file)
        self.master = MasterConfig(options.master_file)
        self.clouds = CloudsConfig(options.clouds_file)
        self.workers = WorkersConfig(options.workers_file)
        self.workload = WorkloadConfig(options.workload_file)
        self.policy = PolicyConfig(options.policy_file)
        self.failuresimulator = FailureSimulatorConfig(options.failuresimulator_file)

        __timestamp = datetime.datetime.now()
        timestamp = __timestamp.strftime("%Y%m%d_%H%M%S")
        self.experiment_id = timestamp
        # Create directory for all the logs
        self.log_dir = "log/%s" % (self.experiment_id)
        os.mkdir(self.log_dir)
        self.remote_log = "%s/%s" % (self.log_dir, options.remote_log)
        self.node_log = "%s/%s" % (self.log_dir, options.node_log)
        self.worker_pool_log = "%s/%s" % (self.log_dir, options.worker_pool_log)
        self.discarded_work_log = "%s/%s" % (self.log_dir, options.discarded_work_log)
        self.failure_log = "%s/%s" % (self.log_dir, options.failure_log)

        # to keep current code running for now
        self.threshold = self.policy.threshold
        self.downscaler_interval = self.policy.downscaler_interval
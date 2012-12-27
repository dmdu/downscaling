import logging

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

class Benchmark(object):
    """ Benchmark class retrieves information from one of the section of the benchmarking file """

    def __init__(self, benchmark_name, config):
        self.config = config
        self.name = benchmark_name
        dict = self.config.items(self.name)
        self.dict = {}
        # Form a dictionary out of items in the specified section
        for pair in dict:
            self.dict[pair[0]] = pair[1]

class BenchmarkingConfig(object):
    """ BenchmarkingConfig class retrieves benchmarking information and populates benchmark list """

    def __init__(self, config):
        self.config = config
        self.list = list()
        for sec in self.config.sections():
            self.list.append(Benchmark(sec, self.config))

class Config(object):
    """ Config class retrieves all configuration information """

    def __init__(self, options):
        #self.clouds = CloudsConfig(read_config(options.clouds_file))
        #self.benchmarking = BenchmarkingConfig(read_config(options.benchmarking_file))

        self.globals = GlobalConfig(options.global_file)
        self.master = MasterConfig(options.master_file)
        self.clouds = CloudsConfig(options.clouds_file)
        self.remote_log = options.remote_log
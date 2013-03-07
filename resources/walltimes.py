import logging

LOG = logging.getLogger(__name__)

class Walltimes(object):

    def __init__(self, config):
        self.config = config

    def get_walltimes_raw(self):
        cmd_walltimes = "condor_q -format \"%d.\" ClusterId -format \"%d \" ProcId -format \"%s\\n\" Args"
        return self.exec_cmd_on_condor_master(cmd_walltimes)

    def save_walltimes_dict(self):
        self.walltimes_dict = {}
        out = self.get_walltimes_raw()
        items = out.split()
        if len(items) % 2:
            LOG.error("Odd number of elements in the job-walltime list")
            return
        for i in range(0, len(items), 2):
            job_id = items[i]
            job_walltime = items[i+1]
            self.walltimes_dict[job_id] = job_walltime
        LOG.info("Walltimes dictionary is saved. Number of jobs: %d" % (len(self.walltimes_dict)))

    
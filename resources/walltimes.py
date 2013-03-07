import logging
from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class Walltimes(object):

    def __init__(self, config, master_dns):
        self.config = config
        self.master_dns = master_dns

    def get_walltimes_raw(self):
        cmd_walltimes = "condor_q -format \"%d.\" ClusterId -format \"%d \" ProcId -format \"%s\\n\" Args"
        return self.exec_cmd_on_condor_master(cmd_walltimes)

    def get_walltimes_dict(self):
        walltimes_dict = {}
        out = self.get_walltimes_raw()
        items = out.split()
        if len(items) % 2:
            LOG.error("Odd number of elements in the job-walltime list")
            return walltimes_dict
        for i in range(0, len(items), 2):
            job_id = items[i]
            job_walltime = items[i+1]
            walltimes_dict[job_id] = job_walltime
        LOG.info("Walltimes dictionary is obtained. Number of jobs: %d" % (len(walltimes_dict)))
        return walltimes_dict

    def exec_cmd_on_condor_master(self, cmd_string):
        cmd = RemoteCommand(
            config = self.config,
            hostname = self.master_dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = cmd_string)
        code = cmd.execute()
        if not code:
            return cmd.stdout
        else:
            LOG.error("Command failed: %s" % (cmd_string))
            LOG.error("Stderr: %s" % (cmd.stderr))
            return None
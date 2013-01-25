import logging
from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class Job(object):

    def __init__(self, id, running, node):

        self.id = id
        self.running = running
        self.node = node

class Jobs(object):

    def __init__(self, config, master_dns):

        self.config = config
        self.master_dns = master_dns
        self.command_job_list =  "condor_q -run | grep %s" % (self.config.workload.user)
        # No -run in the next command, all jobs (including the ones that aren't scheduled yet)
        self.command_job_count = "condor_q | grep %s | wc -l" % (self.config.workload.user)
        self.list = []

    def update_current_list(self):

        rcmd = RemoteCommand(
            config = self.config,
            hostname = self.master_dns,
            ssh_private_key = self.config.globals.priv_path,
            user = self.config.workload.user,
            command = self.command_job_list)
        rcmd.execute()
        queue_state = rcmd.stdout

        self.list = []
        if queue_state != None:
            items = queue_state.split()
            if self.config.workload.user in items:
                start = items.index(self.config.workload.user) - 1
                for i in range(start, len(items), 6):
                    print "Job %s running for %s on %s" % (items[i], items[i+4], items[i+5])
                    self.list.append(Job(items[i], items[i+4], items[i+5]))

    def get_current_number(self):

        rcmd = RemoteCommand(
                config = self.config,
                hostname = self.master_dns,
                ssh_private_key = self.config.globals.priv_path,
                user = self.config.workload.user,
                command = self.command_job_count)
        rcmd.execute()
        curr_number = int(rcmd.stdout)
        return curr_number


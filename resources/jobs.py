import logging
import time
import datetime
from lib.util import RemoteCommand
from resources.walltimes import Walltimes

LOG = logging.getLogger(__name__)

class Job(object):

    def __init__(self, id, running, node, walltime):

        self.id = id
        self.running = running
        self.node = node

        self.walltime = walltime
        self.progress = self.calculate_progress()

    def calculate_progress(self):
        # Walltime should be seconds
        # Convert runtime to seconds. Runtime is reported in format: 0+00:01:40
        if '+' in self.running:
            [d, hms] = self.running.split('+')
            d_seconds = int(d)*86400
            hms_time = time.strptime(hms,'%H:%M:%S')
            hms_seconds = datetime.timedelta(hours=hms_time.tm_hour,minutes=hms_time.tm_min,seconds=hms_time.tm_sec).total_seconds()
            runtime_seconds = d_seconds + hms_seconds

            # reset self.running to be in seconds:
            self.running = int(runtime_seconds)

            if not self.walltime:
                LOG.error("Error in calculating progress for job %s. Walltime is unknown" % (self.id))
                return 0.0
            if float(runtime_seconds) <= float(self.walltime):
                return float(runtime_seconds)/float(self.walltime)
            else:
                LOG.error("Error in calculating progress for job %s. Runtime(%ss) > Walltime(%ss)" % (self.id, str(runtime_seconds), str(self.walltime)))
                return 1.0
        else:
            LOG.error("Error in parsing info for job %s. Can't find '+' in job's runtime: %s"  % (self.id, self.running))
            return 0.0

class Jobs(object):

    def __init__(self, config, master_dns):

        self.config = config
        self.master_dns = master_dns
        self.command_job_list =  "condor_q -run | grep %s" % (self.config.workload.user)
        # No -run in the next command, all jobs (including the ones that aren't scheduled yet)
        self.command_job_count = "condor_q | grep %s | wc -l" % (self.config.workload.user)
        self.list = []

        self.walltimes = Walltimes(config, master_dns)
        self.walltimes_dict = self.walltimes.get_walltimes_dict()

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

                uc = 0
                sdcs = 0
                total = 0
                for i in range(start, len(items), 6):
                    #print "Job %s running for %s on %s" % (items[i], items[i+4], items[i+5])

                    job_id = items[i]
                    if job_id in self.walltimes_dict:
                        walltime = self.walltimes_dict[job_id]
                    else:
                        walltime = None

                    self.list.append(Job(job_id, items[i+4], items[i+5], walltime))

                    node = items[i+5]
                    if "uc" in node:
                        uc += 1
                    if "sdcs" in node:
                        sdcs += 1
                    total += 1

                print "Jobs: total: %d, Hotel: %d, Sierra: %d" % (total, uc, sdcs)

    def get_current_number(self):

        rcmd = RemoteCommand(
                config = self.config,
                hostname = self.master_dns,
                ssh_private_key = self.config.globals.priv_path,
                user = self.config.workload.user,
                command = self.command_job_count)
        rcmd.execute()

        if not rcmd.stdout:
            return 0
        else:
            return int(rcmd.stdout)



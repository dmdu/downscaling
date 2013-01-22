import logging
import time
import os
from threading import Thread

from lib.util import Command, RemoteCommand

LOG = logging.getLogger(__name__)

class Workload(Thread):

    def __init__(self, config, master):

        Thread.__init__(self)
        self.config = config
        self.master = master
        self.batch_files = []
        self.batch_files = os.listdir(self.config.workload.directory)

        if not self.batch_files:
            # Use default workload batch file (def: parsing/condor.submit)
            self.batch_files = [config.workload.submit_local]
        else:
            for batch in self.batch_files:
                # Full path
                batch = "%/%s" % (self.config.workload.directory, batch)

        LOG.info("Workload batches: %s" % str(self.batch_files))

    def run(self):

        for batch in self.batch_files:

            last_line = os.popen("tail -n 1 %s" % batch).read()
            # if sleep time is specified
            if ("SLEEP" in last_line) or ("sleep" in last_line):
                # last item in the line
                sleep_time = int(last_line.split()[1:])
            else:
                sleep_time = 0

            # Copy the batch file to the log directory
            copy_string = "cp %s %s/" % (batch, self.config.log_dir)
            copy_cmd = Command(copy_string)
            code = copy_cmd.execute()
            if code == 0:
                LOG.info("Workload %s file has been copied successfully to the log directory" % (batch))

            # Scp this file to the master
            scp_string = "scp %s %s@%s:~/%s" % (batch, self.config.workload.user, self.master.dns, self.config.workload.submit_remote)
            scp_cmd = Command(scp_string)
            code = scp_cmd.execute()
            if code == 0:
                LOG.info("Batch file %s has been copied to the master node" % (batch))
            else:
                LOG.error("Error occurred during copying batch file %s to the master node" % (batch))

            # Send this batch to the work queue
            exec_cmd = RemoteCommand(
                config = self.config,
                hostname = self.master.dns,
                ssh_private_key = self.config.globals.priv_path,
                user = self.config.workload.user,
                command = 'condor_submit %s' % (self.config.workload.submit_remote))
            code = exec_cmd.execute()
            if code == 0:
                LOG.info("Batch file %s has been submitted to the work queue" % (batch))
            else:
                LOG.error("Error occurred during submission of batch file %s" % (batch))

            # Sleep for a while if this is specified in the batch file
            time.sleep(sleep_time)

            # Done with this file
            self.batch_files.remove(batch)

        # After this for loop, go into monitor mode (run while there are jobs in the queue)
        # TODO: Finish


    def get_log(self):

        # TODO: REVISIT
    #        self.__get_log_command = "scp %s@%s:~/%s" % (
    #            config.workload.user, master.dns, config.workload.log_remote)
        self.get_log_command = "%s %s/sleep.log" % (self.__get_log_command, self.config.log_dir)
        self.get_log_cmd = Command(self.get_log_command)
        code = self.get_log_cmd.execute()
        if code == 0:
            LOG.info("Successfully obtained the log from the master node")
        else:
            LOG.error("Error occurred during obtaining the log from the master node")
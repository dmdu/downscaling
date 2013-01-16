import logging
import datetime
import time

from lib.util import Command, RemoteCommand

LOG = logging.getLogger(__name__)

class Workload(object):

    def __init__(self, config, master):

        self.config = config
        command = "scp %s %s@%s:~/%s" % (
            config.workload.submit_local, config.workload.user,
            master.dns, config.workload.submit_remote)
        self.cmd = Command(command)

        self.rcmd = RemoteCommand(
            config = config,
            hostname = master.dns,
            ssh_private_key = config.globals.priv_path,
            user = config.workload.user,
            command = 'condor_submit %s' % (config.workload.submit_remote))

        self.__get_log_command = "scp %s@%s:~/%s" % (
            config.workload.user, master.dns, config.workload.log_remote)

    def execute(self):

        time.sleep(30)
        code = self.cmd.execute()
        if code == 0:
            LOG.info("Submit file has been copied to the master node")
        else:
            LOG.error("Error occurred during copying submit file to the master node")

        code = self.rcmd.execute()
        if code == 0:
            LOG.info("Workload has been submitted to the queue")
        else:
            LOG.error("Error occurred during workload submission")

    def get_log(self):

        #__timestamp = datetime.datetime.now()
        #timestamp = __timestamp.strftime("%Y%m%d_%H%M%S")
        #self.get_log_command = "%s log/%s.log" % (self.__get_log_command, timestamp)
        self.get_log_command = "%s %s/sleep.log" % (self.__get_log_command, self.config.log_dir)
        self.get_log_cmd = Command(self.get_log_command)

        code = self.get_log_cmd.execute()
        if code == 0:
            LOG.info("Successfully obtained the log from the master node")
        else:
            LOG.error("Error occurred during obtaining the log from the master node")


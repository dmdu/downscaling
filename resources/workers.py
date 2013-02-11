import logging

from lib.util import RemoteCommand

LOG = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, config, instance_id, instance_info):

        self.config = config
        self.instance = instance_id
        self.dns = instance_info['public_dns']

    def terminate_condor(self, master_dns):

        command = "condor_off -fast %s" % (self.dns)
        rcmd = RemoteCommand(
            config = self.config,
            hostname = master_dns,
            ssh_private_key = self.config.globals.priv_path,
            user = 'root',
            command = command)
        code = rcmd.execute()
        if code == 0:
            LOG.info("Successfully stopped Condor daemon on worker %s instance id : %s" % (self.dns, self.instance))
        else:
            LOG.error("Error occurred during Condor daemon termination on worker %s instance: %s" % (self.dns, self.instance))


    def offline(self, master_dns):
        # Marking node offline actually has to be done from the master side

        if master_dns:
            command = "condor_off -peaceful %s" % (self.dns)
            rcmd = RemoteCommand(
                config = self.config,
                hostname = master_dns,
                ssh_private_key = self.config.globals.priv_path,
                user = 'root',
                command = command)
            code = rcmd.execute()
            if code == 0:
                LOG.info("Successfully marked instance offline: %s" % (self.instance))
            else:
                LOG.error("Error occurred during marking instance offline: %s" % (self.instance))
        else:
            LOG.error("Can't mark instance offline without master's dns")

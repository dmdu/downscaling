import logging
import subprocess
import socket
import tempfile
import os
import time
import datetime

from ConfigParser import SafeConfigParser
from optparse import OptionParser
from fabric import api as fabric_api


LOG = logging.getLogger(__name__)


class Command(object):
    def __init__(self, args=[]):
        self.stdout = None
        self.stderr = None
        self.args = args

    def execute(self):
        process = subprocess.Popen(self.args, shell=True,
                                   executable="/bin/bash",
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (self.stdout, self.stderr) = process.communicate()
        return process.returncode


class RemoteCommand(object):
    """Run a command in a remote machine.

    Given a machine address, a none interactive command and ssh key, the function uses fabric
    to execute the command in the remote machines.

    Args:

        hostname (string) : address of the machine

        ssh_private_key (string) : absolute path to ssh private key

        command ( string ) : command to execute


    Return:

        bool

    """

    def __init__(self, config, hostname, ssh_private_key, user, command, retry_interval=5, retry_limit=10):
        self.stdout = None
        self.stderr = None
        self.command = command
        self.hostname = hostname
        self.user = user
        self.ssh_private_key = ssh_private_key
        self.retry_interval = retry_interval
        self.retry_limit = retry_limit
        self.retry_count = 0
        self.remote_log = config.remote_log

    def execute(self):

        self.timestamp = datetime.datetime.now()
        with open(self.remote_log, "a") as log:
            log.write("%s Host: %s, User: %s, CMD: %s\n" %
                      (self.timestamp, self.hostname, self.user, self.command))

        if os.path.isfile(self.ssh_private_key):
            context = fabric_api.settings(fabric_api.hide('running', 'stdout', 'stderr', 'warnings'),
                user=self.user,
                key_filename=[].append(self.ssh_private_key),
                disable_known_hosts=True,
                linewise=True,
                warn_only=True,
                abort_on_prompts=True,
                always_use_pty=True,
                timeout=5,
                use_ssh_config=True)

        else:
            LOG.error("Path to ssh private key is invalid")
            return None

        if context:
            with context:
                fabric_api.env.host_string = self.hostname
                try:
                    results = fabric_api.run(self.command)
                    self.stdout = results.stdout
                    self.stderr = results.stderr

                    with open(self.remote_log, "a") as log:
                        log.write("Error: %s\n" % (self.stderr))
                        log.write("Output: %s\n\n" % (self.stdout))

                    return results.return_code
                except Exception as expt:
                    errmsg = str(expt)
                    LOG.info("Exception in running remote command: %s" % (errmsg))

                    self.retry_count += 1

                    if self.retry_count >= self.retry_limit:
                        LOG.error("Could not execute remote command. Number of retries exceeded the limit")
                        return None

                    time.sleep(self.retry_interval)
                    LOG.info("Trying to execute remote command again. Retry: %d/%d"
                        % (self.retry_count, self.retry_limit))
                    return self.execute()

        else:
            LOG.error("Problem occurred while initializing fabric context")
            return None


def read_config(file):
    config = SafeConfigParser()
    config.read(file)
    return config


def parse_options():
    parser = OptionParser()

    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Enable debugging log level.")
    parser.set_defaults(debug=False)

    parser.add_option("-g", "--global_file", action="store", dest="global_file",
        help="Location of the file with global parameters (default: etc/global.conf).")
    parser.set_defaults(global_file="etc/global.conf")

    parser.add_option("-m", "--master_file", action="store", dest="master_file",
        help="Location of the file with master parameters (default: etc/master.conf).")
    parser.set_defaults(master_file="etc/master.conf")

    parser.add_option("-c", "--clouds_file", action="store", dest="clouds_file",
        help="Location of the file with clouds parameters (default: etc/clouds.conf).")
    parser.set_defaults(clouds_file="etc/clouds.conf")

    parser.add_option("-w", "--workers_file", action="store", dest="workers_file",
        help="Location of the file with workers parameters (default: etc/workers.conf).")
    parser.set_defaults(workers_file="etc/workers.conf")

    parser.add_option("-r", "--remote_log", action="store", dest="remote_log",
        help="Location of the log file for remote command execution (default: log/remote.log).")
    parser.set_defaults(remote_log="log/remote.log")

    (options, args) = parser.parse_args()
    return (options, args)

def check_port_status(address, port=22, timeout=2):
    """Check weather a remote port is accepting connection.

    Given a port and an address, we establish a socket connection
    to determine the port state

    Args :
        address (string): address of the machine, ip or hostname
        port (int) : port number to connect to
        timeout (int) : time to wait for a response

    return :
        bool
            True: if port is accepting connection
            False : if port is not accepting

    """

    default_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    remote_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        remote_socket.connect((address, port))
    except Exception as inst:
        LOG.debug("Exception in check_port_status : %s" % (str(inst)))
        return False
    finally:
        remote_socket.close()
        socket.setdefaulttimeout(default_timeout)
    return True

def clone_git_repo(repo_src):
    """Clone a git repo

    given a repo location, clone it locally and return the directory path

    Args:
        repo_src (string): git repo src location

    Return:
        repo_dest (string): directory that contains the cloned repo

    """
    repo_dest = tempfile.mkdtemp(dir="/tmp")
    clone_cmd_obj = Command("git clone %s %s" % (repo_src, repo_dest))
    if clone_cmd_obj.execute() == 0:
        return repo_dest

def is_executable_file(file_path):
    """Check if a given file is executable

    Args:
        file_path (string) : file absolute path

    Return:
        bool

    """
    return os.path.isfile(file_path) and os.access(file_path, os.X_OK)

def is_yes(input):

    return (input == 'Y' or input == 'y' or input == 'Yes' or input == 'yes')
import logging
import os

from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo

from lib.util import is_yes, printfile

LOG = logging.getLogger(__name__)

class Cloud(object):
    """ Cloud class provides functionality for connecting to a specified cloud and launching an instance there

    cloud_name should match one of the section names in the file that specifies cloud information

    """
    def __init__(self, cloud_name, config):
        self.config = config
        self.name = cloud_name
        self.cloud_config = self.config.clouds.config
        self.cloud_uri = self.cloud_config.get(self.name, "cloud_uri")
        self.cloud_port = int(self.cloud_config.get(self.name, "cloud_port"))
        self.cloud_type = self.cloud_config.get(self.name, "cloud_type")
        self.access_var = self.cloud_config.get(self.name, "access_id").strip('$')
        self.secret_var = self.cloud_config.get(self.name, "secret_key").strip('$')
        self.access_id = os.environ[self.access_var]
        self.secret_key = os.environ[self.secret_var]
        self.conn = None

    def connect(self):
        """ Connects to the cloud using boto interface """

        self.region = RegionInfo(name=self.cloud_type, endpoint=self.cloud_uri)
        self.conn = EC2Connection(
            self.access_id, self.secret_key,
            port=self.cloud_port, region=self.region)
        self.conn.host = self.cloud_uri
        LOG.info("Connected to cloud: %s" % (self.name))

    def register_key(self):
        """ Registers the public key that will be used in the launched instance """

        with open(self.config.globals.pub_path,'r') as key_file_object:
            key_content = key_file_object.read().strip()
        import_result = self.conn.import_key_pair(self.config.globals.key_name, key_content)
        LOG.info("Registered key \"%s\"" % (self.config.globals.key_name))
        return import_result

    def boot_image(self, image_id, count=1, type='m1.small', user_data=None):
        """ Registers the public key and launches count instances of specified image  """

        if self.conn == None:
            self.connect()

        # Check if a key with specified name is already registered. If not, register the key
        registered = False
        for key in self.conn.get_all_key_pairs():
            if key.name == self.config.globals.key_name:
                registered = True
                break
        if not registered:
            self.register_key()
        else:
            LOG.info("Key \"%s\" is already registered" % (self.config.globals.key_name))

        image_object = self.conn.get_image(image_id)
        #print image_id
        boot_result = image_object.run(user_data=user_data, key_name=self.config.globals.key_name,
            min_count=count, max_count=count, instance_type=type)
        LOG.info("Attempted to boot instance(s). Result: %s" % (boot_result))
        return boot_result

    def get_instances(self, exclude_dns=None):
        instances_list = []
        if self.conn == None:
            self.connect()

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            for instance in reservation.instances:
                if not ((exclude_dns) and (instance.public_dns_name == exclude_dns)):
                    instances_list.append(instance)
        return instances_list

    def get_running_instances(self):
        instances_list = []
        if self.conn == None:
            self.connect()

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            for instance in reservation.instances:
                if instance.state == "running":
                    instances_list.append(instance)
        return instances_list

    def get_pending_instances(self):
        instances_list = []
        if self.conn == None:
            self.connect()

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            for instance in reservation.instances:
                if instance.state == "pending":
                    instances_list.append(instance)
        return instances_list


    def terminate_instance(self, instance_id):
        if self.conn == None:
            self.connect()

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            for instance in reservation.instances:
                if instance.id == instance_id:
                    instance.terminate()
                    return True
        return False

    def is_reservation_ready(self, checked_reservation):

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            if reservation.id == checked_reservation.id:
                for instance in reservation.instances:
                    if instance.state == "running":
                        LOG.info("Instance \"%s\" of reservation \"%s\" is running" % (instance.id, reservation.id))
                    else:
                        return False
        return True

    def terminate_all_but_running_instances(self):

        if self.conn == None:
            self.connect()

        all_reservations = self.conn.get_all_instances()
        for reservation in all_reservations:
            for instance in reservation.instances:
                if not (instance.state == "running"):
                    LOG.info("Instance %s in cloud %d isn't running. Terminating it" % (self.name, instance.id))
                    instance.terminate()


class Clouds(object):
    """ Clusters class represents a collection of clouds specified in the clouds file """

    def __init__(self, config):
        self.config = config
        self.list = list()
        for cloud_name in self.config.clouds.list:
            cloud = Cloud(cloud_name, self.config)
            self.list.append(cloud)

    def lookup_by_name(self, name):
        """ Finds a cloud in the collection with a given name; if does not exist, returns None """

        for cloud in self.list:
            if cloud.name == name:
                return cloud
        return None

    def selected_terminate(self):

        terminate = raw_input( "Would you like to terminate running instances now? (Y/N)\n" )
        if is_yes(terminate):

            for cloud in self.list:
                cloud.connect()

            terminate_all = raw_input( "Terminate all running instances? (Y/N)\n" )
            if is_yes(terminate_all):

                for cloud in self.list:
                    if cloud.conn != None:
                        for reservation in cloud.conn.get_all_instances():
                            for instance in reservation.instances:
                                instance.terminate()
                                LOG.info("Terminated instance: %s" % (instance.id))

            else:

                for cloud in self.list:
                    if cloud.conn != None:
                        for reservation in cloud.conn.get_all_instances():
                            for instance in reservation.instances:
                                printfile(self.config.node_log, "Log entries for instance %s:" % instance.id, instance.id)
                                terminate_instance = raw_input("Terminate instance \"%s\"? (Y/N)\n" % (instance.id))
                                if is_yes(terminate_instance):
                                    instance.terminate()
                                    LOG.info("Terminated instance: %s" % (instance.id))
                                else:
                                    LOG.info("Instance \"%s\" is left running" % (instance.id))

            for cloud in self.list:
                cloud.conn = None
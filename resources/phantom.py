import logging
import os
import time
import urlparse

from boto.ec2.connection import EC2Connection
from boto.ec2.regioninfo import RegionInfo
from boto.regioninfo import RegionInfo
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale.launchconfig import LaunchConfiguration
from boto.ec2.autoscale import Tag
from boto.ec2.autoscale.group import AutoScalingGroup
from boto.ec2.connection import EC2Connection

LOG = logging.getLogger(__name__)

class PhantomClient(object):

    def __init__(self, config):

        self.config = config
        self.conn = None

    def connect(self):

        access_id = os.environ[self.config.phantom_config.access_id.strip('$')]
        secret_key = os.environ[self.config.phantom_config.secret_key.strip('$')]

        phantom_region = RegionInfo(name="nimbus", endpoint=self.config.phantom_config.url)
        phantom_connection = AutoScaleConnection(
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
            port=int(self.config.phantom_config.port),
            region=phantom_region,
            is_secure=True)
        self.conn = phantom_connection

    def create_launch_configs(self):

        if self.conn:
            for group in self.config.workers.worker_groups:
                launch_name = "%s@%s" % (self.config.phantom_config.launch_name, group['cloud'])
                LOG.info("Creating launch configuration %s" % (launch_name))
                launch_config = LaunchConfiguration(
                    connection=self.conn,
                    name=launch_name,
                    image_id=group['image_id'],
                    key_name=self.config.globals.key_name,
                    instance_type=group['instance_type'],
                    security_groups=['default'])
                LOG.info("Launch config: %s, %s, %s, %s"
                         % (launch_name, group['image_id'], self.config.globals.key_name, group['instance_type']))

                # delete if exists already
                old_launch_config = self.get_launch_config(launch_name)
                if old_launch_config:
                    LOG.info("Deleting old launch config %s" % (launch_name))
                    self.conn.delete_launch_configuration(old_launch_config.name)
                    time.sleep(30)

                self.conn.create_launch_configuration(launch_config)
        else:
            LOG.error("No connection to Phantom")

    def get_launch_config(self, launch_config_name):

        launch_list = self.conn.get_all_launch_configurations(names=[launch_config_name])
        if launch_list:
            return launch_list[0]
        return None

    def create_auto_scaling_group(self, given_cloud_list=None, given_total_vms=None):

        if self.conn:
            for group in self.config.workers.worker_groups:
                any_cloud_name = group['cloud']
                break
            launch_name = "%s@%s" % (self.config.phantom_config.launch_name, any_cloud_name)
            launch_config = self.get_launch_config(launch_name)
            if not launch_config:
                LOG.error("Phantom can't find launch group %s" % (self.config.phantom_config.launch_name))
                return

            #Example: cloud_list = "hotel:2,sierra:1"
            if not given_cloud_list:
                LOG.info("Creating autoscaling group using workers.conf and initial counts")
                cloud_list = ""
                for group in self.config.workers.worker_groups:
                    cloud_list += "%s:%s," % (group['cloud'], group['initial'])
                # remove trailing comma
                if cloud_list:
                    cloud_list = cloud_list[:-1]
                else:
                    LOG.error("Phantom's cloud list is empty")
                    return
            else:
                LOG.info("Creating autoscaling group using given cloud list: %s" % (given_cloud_list))
                cloud_list = given_cloud_list

            # counting total count
            if not given_total_vms:
                total_vms = 0
                for group in cloud_list.split(','):
                    total_vms += int(group.split(':')[1])
            else:
                # if this parameter is provided, overrule count calculated above
                total_vms = given_total_vms

            LOG.info("Total number of vms for the autoscaling group: %d" % total_vms)

            policy_tag = Tag(
                connection=self.conn,
                key='PHANTOM_DEFINITION',
                value='error_overflow_n_preserving',
                resource_id=self.config.phantom_config.domain_name)
            cloud_tag = Tag(
                connection=self.conn,
                key="clouds",
                value=cloud_list,
                resource_id=self.config.phantom_config.domain_name)
            n_preserving_tag = Tag(
                connection=self.conn,
                key='minimum_vms',
                value=total_vms,
                resource_id=self.config.phantom_config.domain_name)
            tags = [ policy_tag, cloud_tag, n_preserving_tag]

            self.asg = AutoScalingGroup(
                connection=self.conn,
                group_name=self.config.phantom_config.domain_name,
                min_size=total_vms,
                max_size=total_vms,
                launch_config=launch_config,
                tags=tags,
                availability_zones=["us-east-1"])

            # delete old domains
            self.delete_all_domains()

            self.conn.create_auto_scaling_group(self.asg)
        else:
            LOG.error("No connection to Phantom")

    def update_tags(self, given_cloud_list, given_total_vms):

        if self.conn:

            policy_tag = Tag(
                connection=self.conn,
                key='PHANTOM_DEFINITION',
                value='error_overflow_n_preserving',
                resource_id=self.config.phantom_config.domain_name)
            cloud_tag = Tag(
                connection=self.conn,
                key="clouds",
                value=given_cloud_list,
                resource_id=self.config.phantom_config.domain_name)
            n_preserving_tag = Tag(
                connection=self.conn,
                key='minimum_vms',
                value=given_total_vms,
                resource_id=self.config.phantom_config.domain_name)
            tags = [ policy_tag, cloud_tag, n_preserving_tag ]

            LOG.info("Updating tags. New cloud list: %s, new min count: %d" % (given_cloud_list, given_total_vms))
            self.conn.create_or_update_tags(tags)
        else:
            LOG.error("No connection to Phantom")

    def print_info(self):
        if self.conn:

            #print "Launch configurations:"
            #launch_list = self.conn.get_all_launch_configurations()
            #for launch in launch_list:
            #    print "%s, %s, %s, %s, %s" % (launch.name, launch.key_name, launch.instance_type, launch.image_id, launch.created_time)

            #print "Domain information:"
            #all_groups = self.conn.get_all_groups()
            #for group in all_groups:
            #    print "Group: %s, Launch name: %s" % (group.name, group.launch_config_name)

            instances_string = ""
            instance_count = 0
            all_instances = self.conn.get_all_autoscaling_instances()
            for instance in all_instances:
                instances_string += "(%s,%s,%s), " % (instance.instance_id, instance.health_status, instance.lifecycle_state)
                if instance.health_status == "Healthy":
                    instance_count += 1
            LOG.info("Instances: %s" % (instances_string))
            LOG.info("Healthy instance count: %d" % (instance_count))

        else:
            LOG.error("No connection to Phantom")

    def delete_all_launch_config(self):
        if self.conn:
            all_launch_config = self.conn.get_all_launch_configurations()
            #print all_launch_config
            for launch_config in all_launch_config:
                self.conn.delete_launch_configuration(launch_config.name)
        else:
            LOG.error("No connection to Phantom")

    def delete_all_domains(self):
        LOG.info("Deleting all existing domains")
        if self.conn:
            all_domains = self.conn.get_all_groups()
            for domain in all_domains:
                LOG.info("Deleting domain %s" % (domain.name))
                domain.delete()
        else:
            LOG.error("No connection to Phantom")

    def suspend(self):

        LOG.info("Suspending autoscaling group %s" % self.asg.name)
        self.conn.suspend_processes(self.asg)

    def resume(self):

        LOG.info("Resuming autoscaling group %s" % self.asg.name)
        self.conn.resume_processes(self.asg)
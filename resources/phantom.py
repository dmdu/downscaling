import logging
import os
import time

from boto.ec2.regioninfo import RegionInfo
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale.launchconfig import LaunchConfiguration
from boto.ec2.autoscale import Tag
from boto.ec2.autoscale.group import AutoScalingGroup
from resources.clouds import Clouds

LOG = logging.getLogger(__name__)

class PhantomClient(object):

    def __init__(self, config, master):

        self.config = config
        self.conn = None
        self.clouds = Clouds(config)
        self.asg = None
        self.master = master
        self.cloud_list = None


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
                    security_groups=['default'],
                    user_data=self.master.dns)
                LOG.info("Launch config: %s, %s, %s, %s"
                         % (launch_name, group['image_id'], self.config.globals.key_name, group['instance_type']))

                # delete if exists already
                old_launch_config = self.get_launch_config(launch_name)
                if old_launch_config:
                    LOG.info("Deleting old launch config %s" % (launch_name))
                    self.conn.delete_launch_configuration(old_launch_config.name)
                    time.sleep(10)

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
                availability_zones=["us-east-1"],
                desired_capacity=total_vms)

            # delete old domains
            self.delete_all_domains()

            self.conn.create_auto_scaling_group(self.asg)
            self.cloud_list = cloud_list
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

    def terminate_instance(self, instance_id):
        self.conn.terminate_instance(instance_id, decrement_capacity=True)

    def build_instances_info(self,all_instances):
        if not self.conn:
            self.connect()

        instance_dict = {}

        for instance in all_instances:
            instance_dict[instance.instance_id] = {"health_status": instance.health_status,
                                                       "lifecycle_state": instance.lifecycle_state}
            cloud_obj = self.clouds.get_instance_cloud(instance.instance_id)

            if cloud_obj:
                instance_dict[instance.instance_id]['cloud_name'] = cloud_obj.name
                instance_obj = cloud_obj.get_instance_by_id(instance.instance_id)

                if instance_obj:
                    instance_dict[instance.instance_id]["public_dns"] = instance_obj.public_dns_name
        return instance_dict

    def get_alive_instnaces(self, all_instances):
        instance_dict = {}
        for instance in all_instances:
            instance_info = all_instances[instance]
            if 'cloud_name'  in instance_info and  "public_dns" in instance_info:
                instance_dict[instance] = instance_info
        return instance_dict



    def get_autoscale_groups_info(self, asg_name):
        asg_list = self.conn.get_all_groups()
        asg_dict = {}
        for asg in asg_list:
            asg_dict[asg.name] = {}
            asg_dict[asg.name]['launch_config_name'] = asg.launch_config_name
            asg_dict[asg.name]['instances'] = self.build_instances_info(asg.instances)
        return asg_dict


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
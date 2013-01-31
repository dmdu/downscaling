
import logging
import os
import urlparse

from boto.regioninfo import RegionInfo
from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale.launchconfig import LaunchConfiguration
from boto.ec2.autoscale import Tag
from boto.ec2.autoscale.group import AutoScalingGroup
from boto.ec2.connection import EC2Connection


SEC_DIR="/Users/dmdu/.ssh/id_rsa_futuregrid"
#NIMBUS_ACCESS_FILE="%s/.nimbus/querytokens.sh" % (SEC_DIR)

logging.basicConfig(filename="/tmp/boto.log", level=logging.DEBUG)


def build_os_env():
    "to put the credentials in the env"
    #with open(NIMBUS_ACCESS_FILE,'r') as nimubs_access_file:
    #    for line in nimubs_access_file:
    #        if line.strip():
    #            line_as_array = line.split()
    #            key = line_as_array[1].split("=")[0]
    #            value = line_as_array[1].split("=")[1]
    #            os.environ[key] = value

    os.environ['EC2_ACCESS_KEY'] = os.environ['NIMBUS_IAAS_ACCESS_KEY']
    os.environ['EC2_SECRET_KEY'] = os.environ['NIMBUS_IAAS_SECRET_KEY']
    os.environ['PHANTOM_URL'] = "https://svc.uc.futuregrid.org:8445"


def get_cloud_connection(host,port):
    my_access_key = os.environ['NIMBUS_IAAS_ACCESS_KEY']
    my_access_secret = os.environ['NIMBUS_IAAS_SECRET_KEY']
    my_region = RegionInfo(name="nimbus", endpoint=host)
    my_connection = EC2Connection(my_access_key,my_access_secret,port=port,region=my_region)
    my_connection.host = host
    return my_connection

def get_connection():

    my_access_key = os.environ['EC2_ACCESS_KEY']
    my_access_secret = os.environ['EC2_SECRET_KEY']
    phantom_url = os.environ['PHANTOM_URL']

    phantom_url_info = urlparse.urlparse(phantom_url)

    my_region = RegionInfo(name="nimbus", endpoint=phantom_url_info.hostname)
    my_connection = AutoScaleConnection(aws_access_key_id=my_access_key, aws_secret_access_key=my_access_secret, port=phantom_url_info.port, region=my_region, is_secure=True)
    my_connection.host = phantom_url_info.hostname

    return my_connection

def create_launch_config(conn, launch_name, image_name, key_name, instance_type):

    my_launch = LaunchConfiguration(conn, name=launch_name, image_id=image_name, key_name=key_name, instance_type=instance_type, security_groups=['default'])
    return conn.create_launch_configuration(my_launch)

def get_all_launch_config(conn):
    return conn.get_all_launch_configurations()

def get_launch(conn, name):
    launch_list = conn.get_all_launch_configurations(names=[name,])
    if launch_list:
        return launch_list[0]
    return  None

def print_launch_info(conn):
    launch_list = conn.get_all_launch_configurations()
    for launch_config in launch_list:
        print launch_config.name


def delete_launch_config(conn, name):
    conn.delete_launch_configuration(name)

def delete_all_launch_config(conn):
    all_launch_config = conn.get_all_launch_configurations()
    for launch_config in all_launch_config:
        conn.delete_launch_configuration(launch_config.name)

def delete_domain(conn, name):
    domain = conn.get_all_groups(names=[name,])
    domain.delete()

def delete_all_domain(conn):
    all_domain = conn.get_all_groups()
    for domain in all_domain:
        domain.delete()

def print_domain_info(conn):
    all_groups = conn.get_all_groups()
    for group in all_groups:
        print "group name is : %s" % (group.name)
        print "launch name is : %s" % (group.launch_config_name)
        print "Instances:"
        for instance in group.instances:
            print instance.availability_zone

def get_policy_tag(conn, name):
    return Tag(connection=conn, key='PHANTOM_DEFINITION', value='error_overflow_n_preserving', resource_id=name)

def get_clouds_tag(conn, name, clouds_list):
    return Tag(connection=conn, key="clouds", value=clouds_list, resource_id=name)

def get_number_reserve_tag(conn, name, instances_count ):
    return Tag(connection=conn, key='minimum_vms', value=instances_count, resource_id=name)

def create_domian(conn, domain_name, launch_config_name, total_instance_count, clouds_list):

    launch_config = get_launch(conn, launch_config_name)

    if not launch_config:
        print "error in lunch config"
        return

    tags = [ get_policy_tag(conn, domain_name), get_clouds_tag(conn, domain_name, clouds_list), get_number_reserve_tag(conn, domain_name, total_instance_count)]


    group_object = AutoScalingGroup(connection=conn, group_name=domain_name,
        min_size=total_instance_count, max_size=total_instance_count, launch_config=launch_config, tags=tags, availability_zones=["us-east-1"])

    return conn.create_auto_scaling_group(group_object)


def print_auto_scal_group_info(conn):
    print conn.get_all_autoscaling_instances()
    print conn.get_all_groups()

def register_key(connection,key_name,key_path):
    with open(key_path,'r') as key_file_object:
        key_content = key_file_object.read().strip()
    import_result = connection.import_key_pair(key_name, key_content)
    return import_result


def get_all_key_info(connection):
    list_keys = []
    for key in connection.get_all_key_pairs():
        list_keys.append("name : %s, fingerprint: %s" % (key.name, key.fingerprint))
    return list_keys


if __name__ == '__main__':

    # n_preserve : maximum number of VMs to schedule across all the clouds. SetDesiredCapacity change this number.
    # max_size : maximum number of vms in this autoscale group
    # min_size : minumum number of vms in this autoscale gorup
    # "hotel:2,sierra:1" : max vms in hotel are 2, maximum in sierra is 1

    # os env
    build_os_env()

    # build cloud connection to register the key
    cloud_connection = get_cloud_connection('s83r.idp.sdsc.futuregrid.org',8444)
    register_key(cloud_connection,"downscaling","/Users/dmdu/.ssh/id_rsa_futuregrid.pub")
    print get_all_key_info(cloud_connection)

    cloud_connection = get_cloud_connection('svc.uc.futuregrid.org',8444)
    register_key(cloud_connection,"downscaling","/Users/dmdu/.ssh/id_rsa_futuregrid.pub")
    print get_all_key_info(cloud_connection)


    my_connection = get_connection()


    # launch

    #hotel_launch_config = create_launch_config(my_connection, "mylaunch@hotel", "deb5-condor-v09.gz", "downscaling", "m1.small")
    #sierra_launch_config = create_launch_config(my_connection, "mylaunch@sierra", "deb5-condor-v09.gz", "downscaling", "m1.small")
    #mygroup = create_domian(my_connection, "alitest", "mylaunch@hotel", 3, "hotel:2,sierra:1")
    #print mygroup


    # information :
    lcl =  get_all_launch_config(my_connection)
    for lc in lcl:
        print lc.name
        print lc.key_name
        print lc.instance_type
        print lc.image_id
        print lc.created_time

    print "done launch config"
    print_domain_info(my_connection)
    print "done domain info"

    print_auto_scal_group_info(my_connection)

    for group in my_connection.get_all_groups():
        print group.instances
        print group.launch_config_name
        print group.tags

    # reset
    #delete_all_launch_config(my_connection)
    #delete_all_domain(my_connection)

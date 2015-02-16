#! /usr/bin/python

import os
import sys
from boto import ec2
from boto.manage.cmdshell import sshclient_from_instance
import paramiko

bigdataA = os.environ["BIGDATA_HA_HOST_A"]
bigdataB = os.environ["BIGDATA_HA_HOST_B"]
bigdataC = os.environ["BIGDATA_HA_HOST_C"]

hostMap = {}
bigdataHosts = [None] * 3

def createHostAdditions( instances ):
	hostsAdd = "\n"
	for instance in instances:
    		data = instance.__dict__
		if bigdataA in data['tags']['Name']:
			bigdataHosts[0] = instance
			hostsAdd += data[ 'private_ip_address' ] + "\\t" + bigdataA + "\\n"
			hostMap[ bigdataA ] = data[ 'private_ip_address' ]
		elif bigdataB in data['tags']['Name']:
			bigdataHosts[1] = instance
			hostsAdd += data[ 'private_ip_address' ] + "\\t" + bigdataB + "\\n"
			hostMap[ bigdataB ] = data[ 'private_ip_address' ]
		elif bigdataC in data['tags']['Name']:
			bigdataHosts[2] = instance
			hostsAdd += data[ 'private_ip_address' ] + "\\t" + bigdataC + "\\n"
			hostMap[ bigdataC ] = data[ 'private_ip_address' ]

	return hostsAdd

def createZookeeperSubstitution( index, host, ipAddress ):
	return "sudo sed -i 's|server." + index + "=" + host + "|server." + index + "=" + ipAddress + "|' /etc/zookeeper/conf/zoo.cfg"

def createJiniLocatorsSubstitution():
	locators = ""
	vbHostAddresses = [ "33.33.33.10", "33.33.33.11", "33.33.33.12" ]
	index = 0
	for host in hostMap:
		locators = locators + "sudo sed -i 's|" + vbHostAddresses[index] + "|" + hostMap[host] + "|' /etc/default/bigdataHA ;"
		index = index + 1
	locators = locators[:-1]
	return locators

if __name__ == '__main__':

	ec2conn = ec2.connection.EC2Connection( os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"] )
	runningFilter = {'instance-state-name':'running'} # only running states	
	reservations = ec2conn.get_all_instances( filters=runningFilter )
	instances = [i for r in reservations for i in r.instances]

	hostsAdd = createHostAdditions( instances )

	# Create an SSH client for our instance
	#    key_path is the path to the SSH private key associated with instance
	#    user_name is the user to login as on the instance (e.g. ubuntu, ec2-user, etc.)
	key_path = os.environ["AWS_SSH_PRIVATE_KEY"]

	private_security_group_name = os.environ["AWS_SECURITY_GROUP_PRIVATE"]
	group = ec2conn.get_all_security_groups( private_security_group_name )[0]

	jini_locators = createJiniLocatorsSubstitution()
	print "JINI_LOCATORS = " + jini_locators

	i = 1
	for host in bigdataHosts:
		ssh_client = sshclient_from_instance( host, key_path, user_name='ubuntu' )
		# ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

		# Run the command. Returns a tuple consisting of:
		#    The integer status of the command
		#    A string containing the output of the command
		#    A string containing the stderr output of the command
		status, stdin, stderr = ssh_client.run( "sudo sh -c 'echo \"" + hostsAdd + "\" >> /etc/hosts'" )
		status, stdin, stderr = ssh_client.run( "sudo sh -c 'echo " + str(i) + " > /var/lib/zookeeper/myid'" )
		status, stdin, stderr = ssh_client.run( createZookeeperSubstitution( "1", bigdataA, hostMap[ bigdataA ] ) )
		status, stdin, stderr = ssh_client.run( createZookeeperSubstitution( "2", bigdataB, hostMap[ bigdataB ] ) )
		status, stdin, stderr = ssh_client.run( createZookeeperSubstitution( "3", bigdataC, hostMap[ bigdataC ] ) )

		status, stdin, stderr = ssh_client.run( jini_locators )

		hostAddress =  host.__dict__['private_ip_address'] + "/32"
		group.authorize( ip_protocol="tcp", from_port="0", to_port="65535", cidr_ip=hostAddress, src_group=None )

		i += 1
		#
		# startHAServices does not exit as expected, so remote restart commands will hang.
		# As a work around, we restart the host:
		#
		# print "Running: sudo /etc/init.d/zookeeper-server restart on host ", host
		status, stdin, stderr = ssh_client.run( "sudo /etc/init.d/zookeeper-server restart" )
		# print "Running: sudo /etc/init.d/bigdata restart on host ", host
		status, stdin, stderr = ssh_client.run( "sudo /etc/init.d/bigdataHA restart" )
		# status, stdin, stderr = ssh_client.run( "sudo service bigdataHA restart" )
		# host.reboot()

	print "The bigdata HA service is now restarting, this may take several minutes. \nOnce back up, you may confirm status by visiting:\n"
	for host in bigdataHosts:
		print "\thttp://" + host.__dict__['ip_address'] + ":8080/bigdata/status\n"

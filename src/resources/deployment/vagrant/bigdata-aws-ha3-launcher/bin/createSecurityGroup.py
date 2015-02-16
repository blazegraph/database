#! /usr/bin/python

import os
from boto import ec2
from boto.manage.cmdshell import sshclient_from_instance
import paramiko
from datetime import datetime


if __name__ == '__main__':

	# create a security group fo this cluster only.  Just create it now so that it can be associated with the new
	# instance at create time.  Add rules to this group once the instance IP addresses are known.

	ec2conn = ec2.connection.EC2Connection( os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"] )

	group_name = "BDHA " + str( datetime.utcnow() )

	group = ec2conn.create_security_group( group_name, "BigdataHA Security Group" )
 
	envFile = open( ".aws_security_group", "w" ) 
	envFile.write( 'export AWS_SECURITY_GROUP_PRIVATE="' + group_name + '"')

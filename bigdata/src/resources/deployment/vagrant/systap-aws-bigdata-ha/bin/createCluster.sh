#! /bin/sh

export PYTHONPATH=/usr/local/lib/python2.7/site-packages

source aws.rc
python ./bin/createSecurityGroup.py
source .aws_security_group
rm .aws_security_group
vagrant up
echo "Vagrant up completed. Setting host names..."
python ./bin/setHosts.py

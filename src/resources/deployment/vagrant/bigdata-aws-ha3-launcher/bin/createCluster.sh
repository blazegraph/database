#! /bin/sh

export PYTHONPATH=/usr/local/lib/python2.7/site-packages

source aws.rc
python ./bin/createSecurityGroup.py
source .aws_security_group
rm .aws_security_group
vagrant up
#
# Occassionally, usually during svn based builds, AWS has timeout issues.  If this occurs, launch the cluster instances individually:
#
# vagrant up bigdataA
# echo "\nbigdataA is up\n"
# vagrant up bigdataB
# echo "\nbigdataB is up\n"
# vagrant up bigdataC
# echo "\nbigdataC is up\n"
echo "Vagrant up completed. Setting host names..."
python ./bin/setHosts.py

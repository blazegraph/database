REQUIREMENTS
============
This Vagrant resource has been tested against the following versions of required resources:

  Vagrant: 1.4.3
    Vagrant Plugins:
    * nugrant (1.4.2)
    * vagrant-aws (0.4.1)
    * vagrant-berkshelf (1.3.7)

     Chef: 11.10.4
Berkshelf: 2.0.10
   Python: 2.7.5
     Ruby: 1.9.3p448 (2013-06-27 revision 41675) [x86_64-darwin12.3.0]
     Boto: 2.27.0



CONFIGURATION
=============

AWS
---
Your organization's AWS access credentials are essential to launching the cluster.  Please retreive them before attempting to bring up the cluster:

  * AWS Access Key ID
  * AWS Secreet Access Key
  * AWS Keypair Name
  * The SSH Private Key file corresponding to the keypair
  * AWS Security Group for the cluster nodes to join [must minimally allow public TCP access to ports 22 and 8080]


All AWS settings reside in the "aws.rc" file. You must edit this file and set AWS values accordingly.


Vagrant
-------
Vagrant will need the required plugins (see above), if not already installed, they may be added with:

  % vagrant plugin install nugrant
  % vagrant plugin install vagrant-aws
  % vagrant plugin install vagrant-berkshelf


Boto: AWS API
-------------
The "Boto" python library for the AWS API must be installed in order to instantiate the cluster.  If not already installed:

  % sudo pip install pycrypto
  % sudo pip install boto

alternately:

  % sudo easy_install boto


If while running the python scripts the error message appears "ImportError: No module named boto", you will need to set the
PYTHONPATH environment variable, for example:

  % export PYTHONPATH=/usr/local/lib/python2.7/site-packages



LAUNCHING BIGDATA HA CLUSTER
============================

The cluster may be brought up with:

  % ./bin/createCluster.sh

Launching the cluster may take up to 10 minutes.  When complete the cluster creation script will present 


SSH to a specific node:

  % source aws.rc  # all vagrant commands will depend on exported AWS environment variables 
  % vagrant ssh bigdataA


Stop & Start the cluster:

  % vagrant halt
  % vagrant up


Terminating the cluster:

  % vagrant destroy


Trouble Shooting
----------------
If a host is slow to startup there can be an initial connection failure. For example, the bigdataA "status" page may not
appear if bigdataB or bigdataC is slow to start up. In this case log into bigdataA ("vagrant ssh bigdataA") and restart
the service ("sudo /etc/init.d/bigdataA restart") and the host shall connect as expected.


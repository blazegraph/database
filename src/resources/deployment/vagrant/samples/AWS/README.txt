This directory contains a collection of Vagrantfile samples that demonstrate how
to launch an EC2 instance and configure it with a Bigdata server or MapGraph
under several useful scenarios.

The Vagrantfiles are named with a descriptive extension, and must be copied to 
the generic "Vagrantfile" to be read by the vagrant program.  The Vagrantfile
in turn depends on AWS access credentials set in shell environment variables.
These variables are set in the aws.rc file for convenience and must be "sourced"
prior to running vagrant.


Relevant files:
---------------

aws.rc - Set your AWS access credentials here, then:

% source ./aws.rc

This will export your AWS credentials into environment variables that the Vagrant
file will apply to create your EC2 instance.


Vagrantfile.aws.mapgraph - Builds the MapGraph project from its Subversion archive
                      on an Amazon Linux AMI with NVIDIA GRID GPU Driver.

Vagrantfile.aws.tomcat - Creates an EC2 instance (Ubuntu 12.04 by default) and installs
                      Tomcat 7 and deploys the Bigdata WAR file as a service.

Vagrantfile.aws.tomcat.build-from-svn - Like Vagrantfile.aws.tomcat but the Bigdata WAR
                      file will be built from a specified subversion repository branch.

Vagrantfile.aws.nss - Creates an EC2 instance (Ubuntu 12.04 by default) and installs
                      and starts a Bigdata NanoSparqlServer (NSS) Jetty server instance.

Vagrantfile.aws.nss.build-from-svn - Like Vagrantfile.aws.nss but the Bigdata NSS server
                      will be built from a specified subversion repository branch.

Vagrantfile.dual-provider.tomcat - An example file for defining two providers within the
                      same Vagrantfile that will deploy Tomcat and the Bigdata WAR file
                      to the virtual machine instance.  By default the file will create
                      a VirtualBox instance.  To launch an EC2 instance, specify the
                      AWS provider as per:

                        vagrant up --provider=aws


Sample Session
--------------

# edit aws.rc with your favorite editor, this only needs to be done once, then

% source aws.rc
% cp Vagrantfile.aws.tomcat Vagrantfile
% vagrant up

# The bigdata server is now found at the public IP address of the instance: http://<public-ip>:8080/bigdata

# to login to the host:
% vagrant ssh

# to terminate the EC2 instance:
% vagrant destroy

% cp Vagrantfile.aws.tomcat.build-from-svn Vagrantfile
# edit the Vagrantfile and set the :svn_branch variable as desired
% vagrant up


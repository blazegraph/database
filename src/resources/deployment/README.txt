brew - homebrew installer.  installation is the NSS using jetty.  No HA features.

chef - cook book has recipes for bigdata under tomcat; bigdata HA; MapGraph;
       NSS using jetty.

nss  - NSS using jetty. The directory contains shell scripts to (a) control
       the run state of bigdata in an init.d style script; and (b) start the
       NSS using jetty.

vagrant - HA cluster launcher for AWS; MapGraph launcher; NSS using jetty 
      launcher; tomcat + bigdata.war install.

====== Maintenance ======

TODO Rename these things to be less ambiguous once we agree on names.

TODO Document how things are structured from a support and maintenance
perspective. 

TODO Document on the wiki what these various deployments are, how to 
choose the right one, and where to get it. See the following tickets.
Also capture the deployment matrix that Daniel has sent by email.

#926  Add Wiki Entry for Brew Deployment  
#925  Add Wiki Entry for Vagrant Deployments
#924  Add Wiki Entry for Chef Cookbooks

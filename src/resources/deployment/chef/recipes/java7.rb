# http://jamie.mccrindle.org/2013/07/installing-oracle-java-7-using-chef.html
#
# Cookbook Name:: java7
# Recipe:: default
#

apt_repository "webupd8team" do
  uri "http://ppa.launchpad.net/webupd8team/java/ubuntu"
  components ['main']
  distribution node['lsb']['codename']
  keyserver "keyserver.ubuntu.com"
  key "EEA14886"
  deb_src true
end

execute "remove openjdk-6" do
  command "apt-get -y remove --purge openjdk-6-jdk openjdk-6-jre openjdk-6-jre-headless openjdk-6-jre-lib"
end


# could be improved to run only on update
execute "accept-license" do
  command "echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections"
end

package "oracle-java7-installer" do
  action :install
end

package "oracle-java7-set-default" do
  action :install
end

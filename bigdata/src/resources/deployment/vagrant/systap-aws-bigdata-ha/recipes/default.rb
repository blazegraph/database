#
# Cookbook Name:: systap-bigdataHA
# Recipe:: default
#
# Copyright 2014, Systap
#
#


group "bigdata" do
	action :create
	append true
end

user "#{node['systap-bigdataHA'][:bigdata_user]}" do
	gid "#{node['systap-bigdataHA'][:bigdata_group]}"
	supports :manage_home => true
	shell "/bin/false"
	home "#{node['systap-bigdataHA'][:fed_dir]}"
	system true
	action :create
end

# directory node['systap-bigdataHA'][:fed_dir] do
execute "change the ownership of the bigdata home directory to bigdata, which strangely is not" do
 	user "root"
 	group "root"
	cwd "#{node['systap-bigdataHA'][:fed_dir]}"
 	command "chown -R #{node['systap-bigdataHA'][:bigdata_user]}:#{node['systap-bigdataHA'][:bigdata_group]} ."
end

execute "checkout bigdata from svn repo" do
	user 'ubuntu'
 	group 'ubuntu'
	cwd "/home/ubuntu"
        command "svn checkout https://svn.code.sf.net/p/bigdata/code/branches/BIGDATA_RELEASE_1_3_0 #{node['systap-bigdataHA'][:source]}"
end

execute "ant deploy-artifact" do
	user 'ubuntu'
 	group 'ubuntu'
	cwd "#{node['systap-bigdataHA'][:source]}"
	command "ant deploy-artifact"
end

execute "deflate REL tar" do
	user 'bigdata'
 	group 'bigdata'
	cwd "#{node['systap-bigdataHA'][:fed_dir]}/.." 
	command "tar xvf #{node['systap-bigdataHA'][:source]}/REL.bigdata-1.3.0-*.tgz"
end

execute "copy over the /etc/init.d/bigdataHA file" do
	user 'root'
	group 'root'
	cwd "#{node['systap-bigdataHA'][:fed_dir]}/etc/init.d" 
	command "cp bigdataHA /etc/init.d/bigdataHA; chmod 00755 /etc/init.d/bigdataHA"
end

#
# Copy the /etc/init.d/bigdataHA template:
#
# template "/etc/init.d/bigdataHA" do
# 	source  "init.d/bigdataHA.erb"
# 	user 'root'
# 	group 'root'
# 	mode 00755
# end

#
# Create the log directory for bigdata:
#
directory node['systap-bigdataHA'][:log_dir] do
  owner "bigdata"
  group "bigdata"
  mode 00755
  action :create
end

#
# Install the log4jHA.properties file:
#
template  "#{node['systap-bigdataHA'][:fed_dir]}/var/config/logging/log4jHA.properties" do
	source  "log4jHA.properties.erb"
	owner   'bigdata'
	group   'bigdata'
	mode 00644
end

#
# Install the log4jHA.properties file:
#
template  "#{node['systap-bigdataHA'][:fed_dir]}/var/jetty/WEB-INF/jetty.xml" do
	source  "jetty.xml.erb"
	owner   'bigdata'
	group   'bigdata'
	mode 00644
end


#
# Set the absolute path to the RWStore.properties file
#
execute "set absolute path to RWStore.properties" do
	cwd "#{node['systap-bigdataHA'][:fed_dir]}/var/jetty/WEB-INF"
	command "sed -i 's|<param-value>WEB-INF/RWStore.properties|<param-value>#{node['systap-bigdataHA'][:fed_dir]}/var/jetty/WEB-INF/RWStore.properties|' web.xml"
end

#
# Copy the /etc/default/bigdataHA template:
#
template "/etc/default/bigdataHA" do
	source  "default/bigdataHA.erb"
 	user 'root'
 	group 'root'
	mode 00644
end

service "bigdataHA" do
	supports :restart => true, :status => true
	action [ :enable, :start ]
end

#
# Install the zoo.cfg file:
#
template "/etc/zookeeper/conf/zoo.cfg" do
	source  "zoo.cfg.erb"
	owner   'root'
	group   'root'
	mode 00644
end

#
# the hadoop cookbook overlooks the log4j.properties file presently, but a future version may get this right:
#
execute "copy the distribution log4j.properties file" do
	user 'root'
 	group 'root'
	cwd "/etc/zookeeper/conf.chef"
	command "cp ../conf.dist/log4j.properties ."
end

#
# Cookbook Name:: bigdata
# Recipe:: tomcat
#
# Copyright 2014, Systap
#

#
# Only do the following for Bigdata Tomcat install
#
if node['bigdata'][:install_flavor] == "tomcat"

	include_recipe "java"
	include_recipe "tomcat"

	#
	# The tomcat cookbook provides an /etc/default/tomcat7 file that contains multiple JAVA_OPTS lines but allows you to
	# modify only one of them during installation.  As a consequence JAVA_OPTS conflicts may occur.  We comment out the
	# 2nd JAVA_OPTS line to avoid the potential for any conflicts (which do occur with our default java_options attribute).
	#
	# Conflicting collector combinations in option list; please refer to the release notes for the combinations allowed
	# Error: Could not create the Java Virtual Machine.
	#
	execute "comment out 2nd JAVA_OPTS line in /etc/default/tomcat7" do
		cwd	"/etc/default"
		command	"sed -i 's|JAVA_OPTS=\"${JAVA_OPTS} -XX:+UseConcMarkSweepGC\"|#JAVA_OPTS=\"${JAVA_OPTS} -XX:+UseConcMarkSweepGC\"|' tomcat7"
	end


	if node['bigdata'][:build_from_svn]
		include_recipe	"ant"
		include_recipe	"subversion::client"

		#
		# Retrieve the Bigdata source from the specified subversion branch:
		#
		execute "checkout bigdata from svn repo" do
			user 	'ubuntu'
		 	group	'ubuntu'
			cwd	"/home/ubuntu"
		        command	"svn checkout #{node['bigdata'][:svn_branch]} #{node['bigdata'][:source_dir]}"
		end

		#
		# Build the bigdata.war file:
		#
		execute "build the war file" do
			user	'ubuntu'
		 	group	'ubuntu'
			cwd	node['bigdata'][:source_dir]
			command	"ant war"
		end

		#
		# Install the WAR file:
		#
		remote_file "#{node['tomcat'][:webapp_dir]}/bigdata.war" do
			source	"file:///#{node['bigdata'][:source_dir]}/ant-build/bigdata.war"
			owner	node['tomcat'][:user]
			group	node['tomcat'][:group]
		end

	else
		#
		# Install the WAR file from the SourceForge URL:
		#
		remote_file "#{node['tomcat'][:webapp_dir]}/bigdata.war" do
			source	node['bigdata'][:url]
			owner	node['tomcat'][:user]
			group	node['tomcat'][:group]
		end
	end

	#
	# Create the JNL home directory
	#
	directory node['bigdata'][:data_dir] do
		owner	node['tomcat'][:user]
		group	node['tomcat'][:group]
		mode 	00755
		action	:create
		recursive true
	end


	#
	# Create the Bigdata log home
	#
	directory node['bigdata'][:log_dir] do
		owner	node['tomcat'][:user]
		group	node['tomcat'][:group]
		mode	00755
		action	:create
		recursive true
	end


	#
	# Install the RWStore.properties file:
	#
	template node['bigdata'][:properties] do
		source	"RWStore.properties.erb"
		owner	node['tomcat'][:user]
		group	node['tomcat'][:group]
		mode	00644
	end


	#
	# Install the log4j.properties file:
	#
	template node['bigdata'][:log4j_properties] do
		source	"log4j.properties.erb"
		owner	node['tomcat'][:user]
		group	node['tomcat'][:group]
		mode	00644
		retry_delay 15
		retries	3
	end


	#
	# Delete all log files so that the error and warning messages that appeared during the installation
	# process do not unnecessarily alarm anyone.
	#
	execute "remove log files before retart" do
		cwd	"#{node['tomcat'][:log_dir]}"
		command	"rm *"
	end

	#
	# The RWStore.properties path is the only property that needs to be adjusted in the web.xml file. 
	# Using a sed command to adjust the property avoids the need to maintain a web.xml template which
	# in turn updates frequently relative to the other property files.  Thus this recipe becomes
	# suitable against a larger range of bigdata releases.
	#
	if node['bigdata'][:base_version].gsub(/\./, '').to_i >= 131
		#
		# Set the RWStore.properties path in the web.xml file:
		#
		execute "set absolute path for RWStore.properties" do
			cwd	"#{node['bigdata'][:web_home]}/WEB-INF"
			command	"sed -i 's|<param-value>../webapps/bigdata/WEB-INF/RWStore.properties|<param-value>#{node['bigdata'][:home]}/RWStore.properties|' web.xml"
		end

		#
		# Remove original RWStore.properties file to avoid user confusion
		#
		file "#{node['bigdata'][:web_home]}/WEB-INF/RWStore.properties" do
			action :delete
		end
	else
		#
		# 1.3.0 and earlier uses a different path for RWStore.properties.  We can remove this if block in 1.3.1
		#
		execute "set absolute path for RWStore.properties" do
			cwd	"#{node['bigdata'][:web_home]}/WEB-INF"
			command	"sed -i 's|<param-value>../webapps/bigdata/RWStore.properties|<param-value>#{node['bigdata'][:home]}/RWStore.properties|' web.xml"
		end

		#
		# Remove original RWStore.properties file to avoid user confusion
		#
		file "#{node['bigdata'][:web_home]}/RWStore.properties" do
			action	:delete
		end
	end
end

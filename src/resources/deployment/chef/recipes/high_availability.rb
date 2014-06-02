#
# Cookbook Name:: bigdata
# Recipe:: high_availability
#
# Copyright 2014, Systap
#

#
# Only do the following for Bigdata HA install
#
if node['bigdata'][:install_flavor] == "ha"

	include_recipe "java"
	include_recipe "sysstat"
	include_recipe "hadoop::zookeeper_server"

	#
	# Create the bigdata systm group:
	#
	group node['bigdata'][:group] do
		action	:create
		append	true
	end

	#
	# Create the bigdata systm user:
	#
	user node['bigdata'][:user] do
		gid	node['bigdata'][:group]
		supports :manage_home => true
		shell	"/bin/false"
		home	node['bigdata'][:home]
		system	true
		action	:create
	end

	#
	# Make sure the Bigdata home directory is owned by the bigdata user and group:
	#
	execute "change the ownership of the bigdata home directory to bigdata, which strangely is not" do
 		user	"root"
 		group	"root"
		cwd	node['bigdata'][:home]
 		command	"chown -R #{node['bigdata'][:user]}:#{node['bigdata'][:group]} ."
	end

	if node['bigdata'][:build_from_svn]
		include_recipe "ant"
		include_recipe "subversion::client"
		#
		# Retrieve the Bigdata source from the specified subversion branch:
		#
		execute "checkout bigdata from svn repo" do
			user	'ubuntu'
 			group	'ubuntu'
			cwd	"/home/ubuntu"
			command	"svn checkout #{node['bigdata'][:svn_branch]} #{node['bigdata'][:source_dir]}"
		end

		#
		# Build the bigdata release package:
		#
		execute "ant deploy-artifact" do
			user	'ubuntu'
 			group	'ubuntu'
			cwd	node['bigdata'][:source_dir]
			command	"ant deploy-artifact"
		end

		#
		# Extract the just built release package, thus installing it in the Bigdata home directory:
		#
		execute "deflate REL tar" do
			user	node['bigdata'][:user]
 			group	node['bigdata'][:group]
			cwd	"#{node['bigdata'][:home]}/.." 
			command	"tar xvf #{node['bigdata'][:source_dir]}/REL.bigdata-1.*.tgz"
		end

	else
		#
		# Retrieve the package prepared for Brew:
		#
		remote_file "/tmp/bigdata.tgz" do
			owner	node['bigdata'][:user]
			group	node['bigdata'][:group]
			source	node['bigdata'][:url]
		end

		#
		# Extract the just retrieved release package, thus installing it in the Bigdata home directory:
		#

		execute "Extract and relocate the bigdata archive" do
			user	node['bigdata'][:user]
 			group	node['bigdata'][:group]
			cwd	"#{node['bigdata'][:home]}/.." 
			command	"tar xvf /tmp/bigdata.tgz"
		end

		#
		# The following are assumed fixed in releases after 1.3.1 and in the current subversion branch:
		#
		if node['bigdata'][:base_version].gsub(/\./, '').to_i == 131
			execute "Divert standard and error output into /dev/null" do
				user	'root'
				group	'root'
				cwd	"#{node['bigdata'][:home]}/etc/init.d" 
				command	"sed -i 's|startHAServices\"|startHAServices > /dev/null 2>\\&1\"|' bigdataHA"
			end

			execute "Change SystemProperty to Property in the 'host' attribute of jetty.xml" do
				user	'root'
				group	'root'
				cwd	node['bigdata'][:jetty_dir]
			        command "sed -i 's|<Set name=\"host\"><SystemProperty|<Set name=\"host\"><Property|' jetty.xml"
			end

			execute "Change SystemProperty to Property in the 'port' attribute of jetty.xml" do
				user	'root'
				group	'root'
				cwd	node['bigdata'][:jetty_dir]
			        command "sed -i 's|<Set name=\"port\"><SystemProperty|<Set name=\"port\"><Property|' jetty.xml"
			end

			execute "Change SystemProperty to Property in the 'idleTimeout' attribute of jetty.xml" do
				user	'root'
				group	'root'
				cwd	node['bigdata'][:jetty_dir]
			        command "sed -i 's|<Set name=\"idleTimeout\"><SystemProperty|<Set name=\"idleTimeout\"><Property|' jetty.xml"
			end
		end
	end

	#
	# Install hte bigdataHA service file:
	#
	execute "copy over the /etc/init.d/bigdataHA file" do
		user	'root'
		group	'root'
		cwd	"#{node['bigdata'][:home]}/etc/init.d" 
		command	"cp bigdataHA /etc/init.d/bigdataHA; chmod 00755 /etc/init.d/bigdataHA"
	end

	#
	# Create the log directory for bigdata:
	#
	directory node['bigdata'][:log_dir] do
		owner	node['bigdata'][:user]
		group	node['bigdata'][:group]
		mode	00755
		action	:create
	end

	#
	# Install the log4jHA.properties file:
	#
	template "#{node['bigdata'][:home]}/var/config/logging/log4jHA.properties" do
		source	"log4jHA.properties.erb"
		owner	node['bigdata'][:user]
		group	node['bigdata'][:group]
		mode	00644
	end

	#
	# Set the absolute path to the RWStore.properties file
	#
	execute "set absolute path to RWStore.properties" do
		cwd	"#{node['bigdata'][:jetty_dir]}/WEB-INF"
		command	"sed -i 's|<param-value>WEB-INF/RWStore.properties|<param-value>#{node['bigdata'][:properties]}|' web.xml"
	end

	#
	# Install the RWStore.properties file:
	#
	template node['bigdata'][:properties] do
		source	"RWStore.properties.erb"
		owner	node['bigdata'][:user]
		group	node['bigdata'][:group]
		mode	00644
	end

	#
	# Copy the /etc/default/bigdataHA template:
	#
	template "/etc/default/bigdataHA" do
		source	"etc/default/bigdataHA.erb"
 		user	'root'
 		group	'root'
		mode	00644
	end

	#
	# Setup the bigdataHA script as a service:
	#
	service "bigdataHA" do
		supports :restart => true, :status => true
		action [ :enable, :start ]
	end

	#
	# Install the zoo.cfg file:
	#
	template "/etc/zookeeper/conf/zoo.cfg" do
		source	"zoo.cfg.erb"
		owner	'root'
		group	'root'
		mode	00644
	end

	#
	# The hadoop cookbook overlooks the log4j.properties file presently, but a future version may get this right:
	#
	execute "copy the distribution log4j.properties file" do
		user	'root'
 		group	'root'
		cwd	"/etc/zookeeper/conf.chef"
		command	"cp ../conf.dist/log4j.properties ."
	end
end

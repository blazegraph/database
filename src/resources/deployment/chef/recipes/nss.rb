#
# Cookbook Name:: bigdata
# Recipe:: nss
#
# Copyright 2014, Systap
#

#
# Only do the following for Bigdata NSS install
#
if node['bigdata'][:install_flavor] == "nss"

	include_recipe "java"

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


	if node['bigdata'][:build_from_svn]
		include_recipe	"ant"
		include_recipe	"subversion::client"

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
		execute "build the nss tar ball" do
			user	'ubuntu'
		 	group	'ubuntu'
			cwd	node['bigdata'][:source_dir]
			command	"ant package-nss-brew"
		end

		#
		# Extract the just built release package, thus installing it in the Bigdata home directory:
		#
		execute "Extract and relocate the bigdata archive" do
			user	node['bigdata'][:user]
 			group	node['bigdata'][:group]
			cwd	"#{node['bigdata'][:home]}/.." 
			command	"tar xvf #{node['bigdata'][:source_dir]}/REL-NSS.bigdata-1.*.tgz"
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
	end

	#
	# Create a symbolic link of the bin/bigdataNSS script to /etc/init.d/bigdataNSS:
	#
	link "/etc/init.d/bigdataNSS" do
		to "#{node['bigdata'][:home]}/bin/bigdataNSS"
	end

	#
	# Set the install type in the bin/bigdataNSS script:
	#
	execute "set the INSTALL_TYPE in bin/bigdata" do
		cwd	"#{node['bigdata'][:home]}/bin"
		command	"sed -i 's|<%= INSTALL_TYPE %>|#{node['bigdata'][:install_flavor]}|' bigdataNSS"
	end

	#
	# Set the Bigdata home directory in the bin/bigdataNSS file:
	#
	execute "set the BD_HOME in bin/bigdata" do
		cwd	"#{node['bigdata'][:home]}/bin"
		command	"sed -i 's|<%= BD_HOME %>|#{node['bigdata'][:home]}|' bigdataNSS"
	end

	#
	# Set the absolute path to the bigdata.jnl file in RWStore.properties
	#
	execute "set the BD_HOME in RWStore.properties" do
		cwd	"#{node['bigdata'][:jetty_dir]}/WEB-INF"
		command	"sed -i 's|<%= BD_HOME %>|#{node['bigdata'][:home]}|' RWStore.properties"
	end

	#
	# Set the Bigdata home directory in the log4j.properties file to set the path for the log files:
	#
	execute "set the BD_HOME in log4j.properties" do
		cwd	"#{node['bigdata'][:jetty_dir]}/WEB-INF/classes"
		command	"sed -i 's|<%= BD_HOME %>|#{node['bigdata'][:home]}|' log4j.properties"
	end

	#
	# Setup the bigdataNSS script as a service:
	#
	service "bigdataNSS" do
		#
		# Reenable this when the bin/bigdata script is updated to return a "1" for a successful status:
		#
		#   See:  http://comments.gmane.org/gmane.comp.sysutils.chef.user/2723
		#
		# supports :status => true, :start => true, :stop => true, :restart => true
		supports :start => true, :stop => true, :restart => true
		action [ :enable, :start ]
	end
end

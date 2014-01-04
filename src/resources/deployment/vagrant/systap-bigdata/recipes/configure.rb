#
# Cookbook Name:: systap-bigdata
# Recipe:: configure
#
# Copyright 2013, Systap
#
#
include_recipe "tomcat"

directory node['systap-bigdata'][:etc] do
	owner	node['tomcat']['user']
	group	node['tomcat']['group']
	mode 00755
	action :create
	#
	# This is a little hackish.  We need to wait for tomcat to extract the bigdata.war file before we can modify
	# resources within the bigdata folder.  We'll attempt to update this to use the chef notification system later.
	#
	retries 3
	retry_delay 10
end

execute "set absolute path for RWStore.properties" do
	cwd "#{node['systap-bigdata'][:home]}/WEB-INF"
	command "sed -i 's|<param-value>../webapps/bigdata/RWStore.properties|<param-value>#{node['systap-bigdata'][:home]}/RWStore.properties|' web.xml"
end

execute "set path for bigdata.jnl file" do
	cwd "#{node['systap-bigdata'][:home]}"
	command "sed -i 's|=bigdata.jnl|=#{node['systap-bigdata'][:etc]}/bigdata.jnl|' RWStore.properties"
end


execute "set ruleLog in log4j.properties " do
	cwd "#{node['systap-bigdata'][:home]}/WEB-INF/classes"
	command "sed -i 's|log4j.appender.queryLog.File=queryLog.csv|log4j.appender.queryLog.File=#{node['systap-bigdata'][:etc]}/queryLog.csv|' log4j.properties"
end

execute "set ruleLog in log4j.properties " do
	cwd "#{node['systap-bigdata'][:home]}/WEB-INF/classes"
	command "sed -i 's|log4j.appender.ruleLog.File=rules.log|log4j.appender.ruleLog.File=#{node['systap-bigdata'][:etc]}/rules.log|' log4j.properties"
end

execute "set ruleLog in log4j.properties " do
	cwd "#{node['systap-bigdata'][:home]}/WEB-INF/classes"
	command "sed -i 's|log4j.appender.queryRunStateLog.File=queryRunStateLog.csv|log4j.appender.queryRunStateLog.File=#{node['systap-bigdata'][:etc]}/queryRunStateLog.csv|' log4j.properties"
end


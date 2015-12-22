#
# Cookbook Name:: bigdata
# Recipe:: mapgraph
#
# Copyright 2014, Systap
#

#
# MapGraph Installer
#
include_recipe "java"


#
# Make sure the Bigdata home directory is owned by the bigdata user and group:
#
execute "pull mapgraph from svn repo" do
	user	'ec2-user'
 	group	'ec2-user'
	cwd	"/home/ec2-user"
	command	"svn checkout #{node['mapgraph'][:svn_branch]} #{node['mapgraph'][:source_dir]}"
end


#
# Build MapGgraph:
#
execute "make mapgraph" do
 	cwd	node['mapgraph'][:source_dir]
 	command	"make"
end



#
# Run a basic test of MapGraph:
#
execute "test mapgraph" do
 	cwd	node['mapgraph'][:source_dir]
 	command	"./Algorithms/SSSP/SSSP -g smallRegressionGraphs/small.mtx"
end


#
# "recursive true" did not work here
#
# directory node['bigdata'][:mapgraph_home] do
# 	owner 'ec2-user'
# 	group 'ec2-user'
# 	recursive true
# end

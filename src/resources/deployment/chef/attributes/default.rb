#
# Where bigdata resource files will be installed:
#
default['bigdata'][:home] = "/var/lib/bigdata"

#
# Who runs bigdata? This is applicable to NSS and HA installs only:
#
default['bigdata'][:user]  = "bigdata"
default['bigdata'][:group] = "bigdata"
default['bigdata'][:base_version] = "1.3.1"

#
# When "build_from_svn" is "true", code retrieved from subversion will be downloaded to the "source_dir" directory:
#
default['bigdata'][:source_dir] = "/home/ubuntu/bigdata-code"

# Where the RWStore.properties file can be found:
default['bigdata'][:properties] = node['bigdata'][:home] + "/RWStore.properties"

case node['bigdata'][:install_flavor]
when "nss"
	# The URL to the bigdata-nss bundle.  The following is the same bundle used by the Bigdata Brew installer:
	default['bigdata'][:url] = "http://bigdata.com/deploy/bigdata-#{node['bigdata'][:base_version]}.tgz"

	# Where the jetty resourceBase is defined:
	default['bigdata'][:jetty_dir]  = node['bigdata'][:home] + "/var/jetty"

	# Where the log files will live:
	default['bigdata'][:log_dir]  = node['bigdata'][:home] + "/var/log"

	# Where the bigdata-ha.jnl file will live:
	default['bigdata'][:data_dir]  = node['bigdata'][:home] + "/var/data"

	# The subversion branch to use when building from source:
	if node['bigdata'][:build_from_svn]
		default['bigdata'][:svn_branch] = "https://svn.code.sf.net/p/bigdata/code/branches/DEPLOYMENT_BRANCH_1_3_1"
	end
when "tomcat"
	# The Tomcat version to install.  The Bigdata Chef cookbook has only been tested with Version 7:
	default['tomcat'][:base_version] = 7

	# JRE options options to set for Tomcat, the following is strongly recommended:
	default['tomcat'][:java_options] = "-Djava.awt.headless=true -server -Xmx4G -XX:+UseG1GC"

	# A SourceForge URL to use for downloading the bigdata.war file:
	default['bigdata'][:url]  = "http://hivelocity.dl.sourceforge.net/project/bigdata/bigdata/#{node['bigdata'][:base_version]}/bigdata.war"

	# Where the bigdata contents reside under Tomcat:
	default['bigdata'][:web_home] = node['tomcat'][:webapp_dir] + "/bigdata"

	# Where the log4j.properites file can be found:
	default['bigdata'][:log4j_properties] = default['bigdata'][:web_home] + "/WEB-INF/classes/log4j.properties"

	# Where the bigdata-ha.jnl file will live:
	default['bigdata'][:data_dir]  = node['bigdata'][:home] + "/data"

	# Where the log files will live:
	default['bigdata'][:log_dir]  = node['bigdata'][:home] + "/log"

	# The subversion branch to use when building from source:
	if node['bigdata'][:build_from_svn]
		default['bigdata'][:svn_branch] = "https://svn.code.sf.net/p/bigdata/code/branches/BIGDATA_RELEASE_1_3_0"
	end
when "ha"
	# The URL to the bigdataHA release bundle.
	default['bigdata'][:url] = "http://softlayer-dal.dl.sourceforge.net/project/bigdata/bigdata/#{node['bigdata'][:base_version]}/REL.bigdata-#{node['bigdata'][:base_version]}.tgz"

	# The subversion branch to use when building from source:
	if node['bigdata'][:build_from_svn]
		# default['bigdata'][:svn_branch] = "https://svn.code.sf.net/p/bigdata/code/branches/BIGDATA_RELEASE_1_3_0"
		default['bigdata'][:svn_branch] = "https://svn.code.sf.net/p/bigdata/code/branches/DEPLOYMENT_BRANCH_1_3_1"
	end

	# Where the bigdata-ha.jnl file will live:
	default['bigdata'][:data_dir] = node['bigdata'][:home] + "/data"

	# Where the log files will live:
	default['bigdata'][:log_dir] = node['bigdata'][:home] + "/log"

	# Where the jetty resourceBase is defined:
	default['bigdata'][:jetty_dir] = node['bigdata'][:home] + "/var/jetty"

	# Where the RWStore.properties file can be found:
	default['bigdata'][:properties] = node['bigdata'][:jetty_dir] + "/WEB-INF/RWStore.properties"

	# Name of the federation of services (controls the Apache River GROUPS).
	default['bigdata'][:fedname] = 'my-cluster-1'

	# Name of the replication cluster to which this HAJournalServer will belong.
	default['bigdata'][:logical_service_id] = 'HA-Replication-Cluster-1'

	# Set the REPLICATION_FACTOR.  1 = HA1, 3 = HA3, etc
	default['bigdata'][:replication_factor] = 3

	# Where to find the Apache River service registrars (can also use multicast).
	default['bigdata'][:river_locator1] = '33.33.33.10'
	default['bigdata'][:river_locator2] = '33.33.33.11'
	default['bigdata'][:river_locator3] = '33.33.33.12'

	# Where to find the Apache Zookeeper ensemble.
	default['bigdata'][:zk_server1] = 'bigdataA'
	default['bigdata'][:zk_server2] = 'bigdataB'
	default['bigdata'][:zk_server3] = 'bigdataC'

	# set the JVM_OPTS as used by startHAService
	default['bigdata'][:java_options] = "-server -Xmx4G -XX:MaxDirectMemorySize=3000m"
	# default['bigdata'][:java_options] = "-server -Xmx4G -XX:MaxDirectMemorySize=3000m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1046"
end


###################################################################################
#
#  Set the RWStore.properties attributes that apply for all installation scenarios.
#
###################################################################################


default['bigdata']['journal.AbstractJournal.bufferMode'] = "DiskRW"

# Setup for the RWStore recycler rather than session protection.
default['bigdata']['service.AbstractTransactionService.minReleaseAge']= "1"

default['bigdata']['btree.writeRetentionQueue.capacity'] = "4000"
default['bigdata']['btree.BTree.branchingFactor'] = "128"

# 200M initial extent.
default['bigdata']['journal.AbstractJournal.initialExtent'] = "209715200"
default['bigdata']['journal.AbstractJournal.maximumExtent'] = "209715200"

# Setup for QUADS mode without the full text index.
default['bigdata']['rdf.sail.truthMaintenance'] = "false"
default['bigdata']['rdf.store.AbstractTripleStore.quads'] = "false"
default['bigdata']['rdf.store.AbstractTripleStore.statementIdentifiers'] = "false"
default['bigdata']['rdf.store.AbstractTripleStore.textIndex'] = "false"
default['bigdata']['rdf.store.AbstractTripleStore.axiomsClass'] = "com.bigdata.rdf.axioms.NoAxioms"

# Bump up the branching factor for the lexicon indices on the default kb.
default['bigdata']['namespace.kb.lex.com.bigdata.btree.BTree.branchingFactor'] = "400"

# Bump up the branching factor for the statement indices on the default kb.
default['bigdata']['namespace.kb.spo.com.bigdata.btree.BTree.branchingFactor'] = "1024"
default['bigdata']['rdf.sail.bufferCapacity'] = "100000"

#
# Bigdata supports over a hundred properties and only the most commonly configured
# are set here as Chef attributes.  Any number of additional properties may be
# configured by Chef. To do so, add the desired property in this (attributes/default.rb)
# file as well as in the templates/default/RWStore.properties.erb file.  The
# "vocabularyClass" property (below) for inline URIs is used as example additional
# entry:
#
# default['bigdata']['rdf.store.AbstractTripleStore.vocabularyClass'] = "com.my.VocabularyClass"


#################################################################
#
#  The following attributes are defaults for the MapGraph recipe.
#
#################################################################

# The subversion branch to use when building from source:
default['mapgraph'][:svn_branch] = "https://svn.code.sf.net/p/mpgraph/code/trunk"

# MapGraph code retrieved from subversion will be downloaded to the "source_dir" directory:
default['mapgraph'][:source_dir] = "/home/ec2-user/mapgraph-code"

default['tomcat']['base_version'] = 7
default['systap-bigdata'][:url]  = "http://sourceforge.net/projects/bigdata/files/bigdata/1.3.0/bigdata.war/download"

webapp_dir = node['tomcat']['webapp_dir']
default['systap-bigdata'][:home] = webapp_dir + "/bigdata"
default['systap-bigdata'][:etc]  = webapp_dir + "/bigdata/etc"

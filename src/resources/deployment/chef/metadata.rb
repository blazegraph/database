name		'bigdata'
maintainer	'Daniel Mekonnen'
maintainer_email 'daniel<no-spam-at>systap.com'
license		'GNU GPLv2'
description	'Installs/Configures Systap Bigdata High Availability'
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version		'0.1.5'

depends		'apt'
depends		'java', '>= 1.22.0' 
depends		'ant'
depends		'tomcat'
depends		'subversion'
depends		'lvm'
depends		'hadoop'
depends		'emacs'
depends		'sysstat'

supports	'ubuntu'

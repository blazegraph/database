# -*- mode: ruby -*-
# vi: set ft=ruby :
#
# Vagrantfile.aws.nss - Install Bigdata NanoSparqlServer with an AWS Provider
#
# The launch synopsis for this Vagrantfile:
#
#   % source ./aws.rc
#   % vagrant up
#
ENV['VAGRANT_DEFAULT_PROVIDER'] = 'aws'

Vagrant.require_plugin "vagrant-berkshelf"

Vagrant.configure("2") do |config|

  config.vm.box = "dummy"
  config.vm.hostname = ENV['BIGDATA_HA_HOST_A']

  config.berkshelf.enabled = true

  config.vm.provider :aws do |aws, override|
    aws.access_key_id = ENV['AWS_ACCESS_KEY_ID']
    aws.secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    aws.keypair_name = ENV['AWS_KEYPAIR_NAME']

    aws.ami = ENV['AWS_AMI']
    
    aws.region = ENV['AWS_REGION']
    aws.instance_type = ENV['AWS_INSTANCE_TYPE']
    aws.security_groups = [ ENV['AWS_SECURITY_GROUPS'], ENV['AWS_SECURITY_GROUP_PRIVATE'] ]

    aws.tags = {
      'Name' => ENV['BIGDATA_HA_HOST_A']
    }

    override.ssh.username = ENV['AWS_AMI_USERNAME']
    override.ssh.private_key_path = ENV['AWS_SSH_PRIVATE_KEY']
  end


  config.vm.provision :chef_solo do |chef|
    chef.json = {
	:bigdata => {
	 	:install_flavor => "nss",
	 	:build_from_svn => false
	},
	:java => {
		"install_flavor" => "oracle",
		:jdk_version => "7",
		:oracle => { 'accept_oracle_download_terms' => true }
	},
	:tomcat => {
	 	:base_version => "7"
	}
    }

    config.vm.provision :shell, inline: "sudo apt-get update ; sudo curl -L https://www.opscode.com/chef/install.sh | sudo bash"

    chef.run_list = [
        "recipe[bigdata::nss]"
    ]

  end
end

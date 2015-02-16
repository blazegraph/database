# -*- mode: ruby -*-
# vi: set ft=ruby :
#
# Vagrantfile.nss - Install Bigdata NanoSparqlServer with a VirtualBox (Default) Provider
#
# The launch synopsis for this Vagrantfile:
#
#   % vagrant up
#

Vagrant.require_plugin "vagrant-berkshelf"

Vagrant.configure("2") do |config|

  config.vm.hostname = "bigdata"
  config.vm.box = "precise64"

  config.berkshelf.enabled = true

  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  config.vm.network :private_network, ip: "33.33.33.10"


  config.vm.provision :chef_solo do |chef|
    chef.json = {
	:bigdata => {
	 	:install_flavor => "nss",
	 	:build_from_svn => false
	},
	:java => {
		:install_flavor => "oracle",
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

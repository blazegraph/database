#
# Cookbook Name:: bigdata
# Recipe:: ssd
#
# Copyright 2014, Systap
#

#
#  SSD Setup
#
include_recipe "lvm"


#
# Create the directory that will be the mount target:
#
directory node['bigdata'][:data_dir] do
	owner	"root"
	group	"root"
	mode	00755
	action	:create
	recursive true
end


#
# Create and mount the logical volume:
#
lvm_volume_group 'vg' do
  action :create
  physical_volumes ['/dev/xvdb', '/dev/xvdc']

  logical_volume 'lv_bigdata' do
    size	'100%VG'
    filesystem	'ext4'
    mount_point	location: node['bigdata'][:data_dir], options: 'noatime,nodiratime'
    # stripes	4
  end
end

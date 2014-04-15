#
# Cookbook Name:: planx-aws-bigdata-rdr 
# Recipe:: ssd
#
include_recipe "lvm"

#
#  SSD Setup
#
directory node['systap-bigdataHA'][:data_dir] do
	owner	"root"
	group	"root"
	mode 00755
	action :create
	recursive true
end


lvm_volume_group 'vg' do
  action :create
  physical_volumes ['/dev/xvdb', '/dev/xvdc']

  logical_volume 'lv_bigdata' do
    size        '100%VG'
    filesystem  'ext4'
    mount_point location: node['systap-bigdataHA'][:data_dir], options: 'noatime,nodiratime'
    # stripes     4
  end
end

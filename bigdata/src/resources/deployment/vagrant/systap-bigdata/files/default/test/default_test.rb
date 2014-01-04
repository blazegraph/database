require 'minitest/spec'
describe_recipe 'systap-bigdata::test' do
  it "is running the tomcat server" do
    service('tomcat').must_be_running
  end
end

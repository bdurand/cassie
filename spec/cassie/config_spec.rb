require "spec_helper"

describe Cassie::Config do
  
  let(:options) do
    {
      "consistency" => :one,
      "timeout" => 15,
      "schema_directory" => "/tmp",
      "max_prepared_statements" => 100,
      "keyspaces" => {"default" => "test_default", "other" => "test_other"}
    }
  end
  
  it "should load the connection options" do
    config = Cassie::Config.new(options)
    config.cluster_options.should == {:consistency => :one, :timeout => 15}
  end
  
  it "should load the keyspace" do
    config = Cassie::Config.new(options)
    config.keyspace(:default).should start_with("test_default")
    config.keyspace("other").should start_with("test_other")
  end
  
  it "should get the schema_directory" do
    config = Cassie::Config.new(options)
    config.schema_directory.should == "/tmp"
    Cassie::Config.new({}).schema_directory.should == nil
  end
  
  it "should get the max_prepared_statements" do
    config = Cassie::Config.new(options)
    config.max_prepared_statements.should == 100
    Cassie::Config.new({}).max_prepared_statements.should == 1000
  end
  
end

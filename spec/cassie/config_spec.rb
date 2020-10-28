require "spec_helper"

describe Cassie::Config do
  let(:options) do
    {
      "cluster" => {
        "consistency" => :one,
        "timeout" => 15
      },
      "schema_directory" => "/tmp",
      "max_prepared_statements" => 100,
      "keyspaces" => {"default" => "test_default", "other" => "test_other"},
      "default_keyspace" => "another"
    }
  end

  it "should handle empty options" do
    config = Cassie::Config.new({})
    expect(config.cluster).to eq({})
    expect(config.keyspace_names).to eq([])
    expect(config.default_keyspace).to eq(nil)
    expect(config.schema_directory).to eq(nil)
    expect(config.max_prepared_statements).to eq(1000)
  end

  it "should have cluster options" do
    config = Cassie::Config.new(options)
    expect(config.cluster).to eq({consistency: :one, timeout: 15})
  end

  it "should have keyspaces" do
    config = Cassie::Config.new(options)
    expect(config.keyspace(:default)).to start_with("test_default")
    expect(config.keyspace("other")).to start_with("test_other")
    expect(config.keyspace_names).to match_array(["default", "other"])
  end

  it "should have a default_keyspace" do
    config = Cassie::Config.new(options)
    expect(config.default_keyspace).to eq("another")
  end

  it "should get the schema_directory" do
    config = Cassie::Config.new(options)
    expect(config.schema_directory).to eq("/tmp")
    expect(Cassie::Config.new({}).schema_directory).to eq(nil)
  end

  it "should get the max_prepared_statements" do
    config = Cassie::Config.new(options)
    expect(config.max_prepared_statements).to eq(100)
    expect(Cassie::Config.new({}).max_prepared_statements).to eq(1000)
  end
end

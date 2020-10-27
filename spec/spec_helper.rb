require 'rubygems'

require 'active_model'

require_relative '../lib/whi-cassie'

require_relative 'models/thing'
require_relative 'models/type_tester'

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus

  # Run specs in random order to surface order dependencies. If you find an
  # order dependency and want to debug it, you can fix the order by providing
  # the seed, which is printed after each run.
  #     --seed 1234
  config.order = 'random'

  config.expect_with(:rspec) { |c| c.syntax = [:should, :expect] }

  cassandra_host, cassandra_port = ENV.fetch("CASSANDRA_HOST", "localhost").split(":", 2)
  cassandra_port ||= 9042
  config.before(:suite) do
    schema_dir = File.expand_path("../schema", __FILE__)
    protocol_version = (ENV["protocol_version"] ? ENV["protocol_version"].to_i : 3)
    Cassie.configure!(
      :cluster => {:host => cassandra_host, port: cassandra_port.to_i, :protocol_version => protocol_version,},
      :keyspaces => {"test" => "cassie_specs"},
      :schema_directory => schema_dir,
      :max_prepared_statements => 3
    )
    Cassie::Schema.load_all!
    Cassie::Testing.prepare!
  end

  config.after(:suite) do
    Cassie::Schema.drop_all!
  end

  config.around(:each) do |example|
    Cassie::Testing.cleanup! do
      example.run
    end
  end
end

require 'cassie'
require File.expand_path('../models/thing', __FILE__)
require File.expand_path('../models/type_tester', __FILE__)

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus

  # Run specs in random order to surface order dependencies. If you find an
  # order dependency and want to debug it, you can fix the order by providing
  # the seed, which is printed after each run.
  #     --seed 1234
  config.order = 'random'
  
  config.expect_with(:rspec) { |c| c.syntax = [:should, :expect] }
  
  config.before(:suite) do
    schema_dir = File.expand_path("../schema", __FILE__)
    Cassie.configure!(:cluster => {:host => 'localhost'}, :keyspaces => {"test" => "cassie_specs"}, :schema_directory => schema_dir, :max_prepared_statements => 3)
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

# frozen_string_literal: true

# Initialize Cassie instance with default behaviors for a Rails environment.
#
# Configuration will be gotten from config/cassie.yml.
#
# Schema location will be set to db/cassandra for development and test environments.
class Cassie::Railtie < Rails::Railtie
  initializer "cassie.initialization" do
    Cassie.logger = Rails.logger
    
    config_file = Rails.root + 'config' + 'cassie.yml'
    if config_file.exist?
      options = YAML::load(ERB.new(config_file.read).result)[Rails.env]
      if Rails.env.development? || Rails.env.test?
        schema_dir = Rails.root + 'db' + 'cassandra'
        options['schema_directory'] = schema_dir.to_s if schema_dir.exist?
      end
      Cassie.configure!(options)
    end
  end
end

# Simple configuration for connecting to Cassandra.
#
# keyspaces are a map of abstract keyspace names to actual names. These can be used in lieu of hard
# coding keyspace names and can be especially useful if keyspaces differ between environments. The
# abstract names can then be used when defining the keyspace for a model.
#
# default_keyspace is an optional keyspace name to use as the default. It can be either the actual
# name or an abstract name mapped to an actual name in the keyspaces map.
#
# max_prepared_statements is the maximum number of prepared statements that will be kept cached on
# the client (default 1000).
#
# schema_directory is an optional path to the location where you schema files are stored. This should
# only be set in development and test environments since schema statements can be destructive in
# production.
#
# All other values are sent to Cassandra.cluster to initialize the cluster. See documentation in
# cassandra-driver gem for Cassandra.cluster.
class Cassie::Config
  attr_reader :cluster_options
  attr_accessor :max_prepared_statements, :schema_directory, :default_keyspace
  
  def initialize(options = {})
    options = options.symbolize_keys
    @keyspaces = (options.delete(:keyspaces) || {}).stringify_keys
    @max_prepared_statements = options.delete(:max_prepared_statements) || 1000
    @schema_directory = options.delete(:schema_directory)
    @default_keyspace = options.delete(:default_keyspace)
    @cluster_options = options
  end
  
  # Get the actual keyspace mapped to the abstract name.
  def keyspace(name)
    @keyspaces[name.to_s] || name.to_s
  end
  
  # Get the list of keyspaces defined for the cluster.
  def keyspaces
    @keyspaces.values
  end
  
  # Get the list of abstract keyspace names.
  def keyspace_names
    @keyspaces.keys
  end
  
  # Add a mapping of a name to a keyspace.
  def add_keyspace(name, value)
    @keyspaces[name.to_s] = value
  end
  
  # Return the cluster options without passwords or tokens. Used for logging.
  def sanitized_cluster_options
    options = cluster_options.dup
    options[:password] = "SUPPRESSED" if options.include?(:password)
    options[:passphrase] = "SUPPRESSED" if options.include?(:passphrase)
    options[:credentials] = "SUPPRESSED" if options.include?(:credentials)
    options[:auth_provider] = "SUPPRESSED" if options.include?(:auth_provider)
    options
  end
  
end

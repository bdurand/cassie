# This class can be used to create, drop, or get information about the cassandra schemas. This class
# is intended only to provide support for creating schemas in development and test environments. You
# should not use this class with your production environment since some of the methods can be destructive.
#
# The schemas are organized by keyspace.
#
# To load schemas for test and development environments you should specify a directory where the schema
# definition files live. The files should be named "#{abstract_keyspace}.cql". The actual keyspace name will
# be looked from the keyspace mapping in the configuration.
class Cassie::Schema
  TABLES_CQL = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?".freeze
  
  CREATE_MATCHER = /\A(?<create>CREATE (TABLE|((CUSTOM )?INDEX)|TYPE|TRIGGER))(?<exist>( IF NOT EXISTS)?) (?<object>[a-z0-9_.]+)/i.freeze
  DROP_MATCHER = /\A(?<drop>DROP (TABLE|INDEX|TYPE|TRIGGER))(?<exist>( IF EXISTS)?) (?<object>[a-z0-9_.]+)/i.freeze
  
  attr_reader :keyspace
  
  class << self
    # Get all the defined schemas.
    def all
      schemas.values
    end
    
    # Find the schema for a keyspace using the abstract name.
    def find(keyspace)
      schemas[keyspace]
    end
    
    # Throw out the cached schemas so they can be reloaded from the configuration.
    def reset!
      @schemas = nil
    end
    
    # Drop a specified keyspace by abstract name. The actual keyspace name will be looked up
    # from the keyspaces in the configuration.
    def drop!(keyspace_name)
      keyspace = Cassie.instance.config.keyspace(keyspace_name)
      raise ArgumentError.new("#{keyspace_name} is not defined as keyspace in the configuration") unless keyspace
      
      drop_keyspace_cql = "DROP KEYSPACE IF EXISTS #{keyspace}"
      Cassie.instance.execute(drop_keyspace_cql)
    end
    
    # Load a specified keyspace by abstract name. The actual keyspace name will be looked up
    # from the keyspaces in the configuration.
    def load!(keyspace_name)
      keyspace = Cassie.instance.config.keyspace(keyspace_name)
      raise ArgumentError.new("#{keyspace_name} is not defined as keyspace in the configuration") unless keyspace

      schema_file = File.join(Cassie.instance.config.schema_directory, "#{keyspace_name}.cql")
      raise ArgumentError.new("#{keyspace_name} schema file does not exist at #{schema_file}") unless File.exist?(schema_file)
      schema_statements = File.read(schema_file).split(';').collect{|s| s.strip.chomp(';')}
      
      create_keyspace_cql = "CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}"
      Cassie.instance.execute(create_keyspace_cql)
      
      schema_statements.each do |statement|
        statement = statement.gsub(/#(.*)$/, '').gsub(/\s+/, ' ').strip
        create_match = statement.match(CREATE_MATCHER)
        if create_match
          object = create_match["object"]
          object = "#{keyspace}.#{object}" unless object.include?('.')
          statement = statement.sub(create_match.to_s, "#{create_match['create']} IF NOT EXISTS #{object}")
        else
          drop_match = statement.match(DROP_MATCHER)
          if drop_match
            object = drop_match["object"]
            object = "#{keyspace}.#{object}" unless object.include?('.')
            statement = statement.sub(drop_match.to_s, "#{drop_match['drop']} IF EXISTS #{object}")
          end
        end
        unless statement.blank?
          Cassie.instance.execute(statement)
        end
      end
      nil
    end
    
    # Drop all keyspaces defined in the configuration.
    def drop_all!
      Cassie.instance.config.keyspace_names.each do |keyspace|
        drop!(keyspace)
      end
    end
    
    # Drop all keyspaces defined in the configuration.
    def load_all!
      Cassie.instance.config.keyspace_names.each do |keyspace|
        load!(keyspace)
      end
    end
    
    private
    
    def schemas
      unless defined?(@schemas) && @schemas
        schemas = {}
        Cassie.instance.config.keyspaces.each do |keyspace|
          schemas[keyspace] = new(keyspace)
        end
        @schemas = schemas
      end
      @schemas
    end
  end
  
  def initialize(keyspace)
    @keyspace = keyspace
  end
  
  # Returns a list of tables defined for the schema.
  def tables
    unless defined?(@tables) && @tables
      tables = []
      results = Cassie.instance.execute(TABLES_CQL, keyspace)
      results.each do |row|
        tables << row['columnfamily_name']
      end
      @tables = tables
    end
    @tables
  end
  
  # Truncate the data from a table.
  def truncate!(table)
    statement = Cassie.instance.prepare("TRUNCATE #{keyspace}.#{table}")
    Cassie.instance.execute(statement)
  end
end

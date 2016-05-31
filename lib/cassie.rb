require 'cassandra'

# This class provides a lightweight wrapper around the Cassandra driver. It provides
# a foundation for maintaining a connection and constructing CQL statements.
class Cassie
  require File.expand_path("../cassie/config.rb", __FILE__)
  require File.expand_path("../cassie/model.rb", __FILE__)
  require File.expand_path("../cassie/schema.rb", __FILE__)
  require File.expand_path("../cassie/testing.rb", __FILE__)
  require File.expand_path("../cassie/railtie.rb", __FILE__) if defined?(Rails)
  
  class RecordNotFound < StandardError
  end
  
  class RecordInvalid < StandardError
    attr_reader :record
    
    def initialize(record)
      super("Errors on #{record.class.name}: #{record.errors.to_hash.inspect}")
      @record = record
    end
  end
  
  # Message passed to subscribers with the statement, options, and time for each statement
  # to execute. Note that if statements are batched they will be packed into one message
  # with a Cassandra::Statements::Batch statement and empty options.
  class Message
    attr_reader :statement, :options, :elapsed_time
    
    def initialize(statement, options, elapsed_time)
      @statement = statement
      @options = options
      @elapsed_time = elapsed_time
    end
  end
  
  attr_reader :config, :subscribers
  
  class << self
    # A singleton instance that can be shared to communicate with a Cassandra cluster.
    def instance
      unless defined?(@instance) && @instance
        instance = new(@config)
        @instance = instance
      end
      @instance
    end
    
    # Call this method to load the Cassie::Config from the specified file for the
    # specified environment.
    def configure!(options)
      if defined?(@instance) && @instance
        old_instance = @instance
        @instance = nil
        old_instance.disconnect
      end
      @config = Cassie::Config.new(options)
    end
    
    # This method can be used to set a consistency level for all Cassandra queries
    # within a block that don't explicitly define them. It can be used where consistency
    # is important (i.e. on validation queries) but where a higher level method
    # doesn't provide an option to set it.
    def consistency(level)
      save_val = Thread.current[:cassie_consistency]
      begin
        Thread.current[:cassie_consistency] = level
        yield
      ensure
        Thread.current[:cassie_consistency] = save_val
      end
    end
    
    # Get a Logger compatible object if it has been set.
    def logger
      @logger if defined?(@logger)
    end
    
    # Set a logger with a Logger compatible object.
    def logger=(value)
      @logger = value
    end
  end
  
  def initialize(config)
    @config = config
    @monitor = Monitor.new
    @session = nil
    @prepared_statements = {}
    @last_prepare_warning = Time.now
    @subscribers = []
  end
  
  # Open a connection to the Cassandra cluster.
  def connect
    start_time = Time.now
    cluster_config = config.cluster
    cluster_config = cluster_config.merge(:logger => logger) if logger
    cluster = Cassandra.cluster(cluster_config)
    logger.info("Cassie.connect with #{config.sanitized_cluster} in #{((Time.now - start_time) * 1000).round}ms") if logger
    @monitor.synchronize do
      @session = cluster.connect(config.default_keyspace)
      @prepared_statements = {}
    end
  end

  # Close the connections to the Cassandra cluster.
  def disconnect
    logger.info("Cassie.disconnect from #{config.sanitized_cluster}") if logger
    @monitor.synchronize do
      @session.close if @session
      @session = nil
      @prepared_statements = {}
    end
  end
  
  # Return true if the connection to the Cassandra cluster has been established.
  def connected?
    !!@session
  end
  
  # Force reconnection. If you're using this code in conjunction in a forking server environment
  # like passenger or unicorn you should call this method after forking.
  def reconnect
    disconnect
    connect
  end

  # Prepare a CQL statement for repeate execution. Prepared statements
  # are cached on the driver until the connection is closed. Calling
  # prepare multiple times with the same CQL string will return
  # the prepared statement from a cache.
  def prepare(cql)
    raise ArgumentError.new("CQL must be a string") unless cql.is_a?(String)
    statement = @prepared_statements[cql]
    cache_filled_up = false
    unless statement
      @monitor.synchronize do
        statement = session.prepare(cql)
        @prepared_statements[cql] = statement
        if @prepared_statements.size > config.max_prepared_statements
          # Cache is full. Clear out the oldest values. Ideally we'd remove the least recently used,
          # but that would require additional overhead on each query. This method will eventually
          # keep the most active queries in the cache and is overall more efficient.
          @prepared_statements.delete(@prepared_statements.first[0])
          cache_filled_up = true
        end
      end
    end
    
    if cache_filled_up && logger && Time.now > @last_prepare_warning + 10
      # Set a throttle on how often this message is logged so we don't kill performance enven more.
      @last_prepare_warning = Time.now
      logger.warn("Cassie.prepare cache filled up. Consider increasing the size from #{config.max_prepared_statements}.")
    end
    
    statement
  end

  # Declare and execute a batch statement. Any insert, update, or delete
  # calls made within the block will add themselves to the batch which
  # is executed at the end of the block.
  def batch(options = nil)
    if Thread.current[:cassie_batch]
      yield
    else
      begin
        batch = []
        Thread.current[:cassie_batch] = batch
        yield
        unless batch.empty?
          batch_statement = session.logged_batch
          batch.each do |cql, values|
            if values.blank?
              batch_statement.add(cql)
            else
              statement = prepare(cql)
              statement = statement.bind(Array(values)) if values.present?
              batch_statement.add(statement)
            end
          end
          execute(batch_statement)
        end
      ensure
        Thread.current[:cassie_batch] = nil
      end
    end
  end

  # Find rows using the CQL statement. If the statement is a string
  # and values are provided then the statement will executed as a prepared
  # statement. In general all statements should be executed this way.
  #
  # If you have a statement without arguments, then you should call
  # prepare before and pass the prepared statement if you plan on
  # executing the same query multiple times.
  def find(cql, values = nil, options = nil)
    execute(cql, values, options)
  end

  # Insert a row from a hash into a table.
  #
  # You can specify a ttl for the created row by supplying a :ttl option.
  #
  # If this method is called inside a batch block it will be executed in the batch.
  def insert(table, values_hash, options = nil)
    columns = []
    values = []
    values_hash.each do |column, value|
      if !value.nil?
        columns << column
        values << value
      end
    end
    cql = "INSERT INTO #{table} (#{columns.join(', ')}) VALUES (#{question_marks(columns.size)})"
    
    ttl = options[:ttl] if options
    if ttl
      cql << " USING TTL ?"
      values << ttl
    end
    
    batch_or_execute(cql, values, options)
  end

  # Update a row in a table. The values to update should be passed in the
  # values_hash while the primary key should be passed in the key_hash.
  #
  # You can specify a ttl for the created row by supplying a :ttl option.
  #
  # If this method is called inside a batch block it will be executed in the batch.
  def update(table, values_hash, key_hash, options = nil)
    key_cql, key_values = key_clause(key_hash)
    update_cql = []
    update_values = []
    if values_hash.is_a?(String)
      update_cql << values_hash
    else
      values_hash.each do |column, value|
        update_cql << "#{column} = ?"
        update_values << value
      end
    end
    values = update_values + key_values
    
    cql = "UPDATE #{table}"
    ttl = options[:ttl] if options
    if ttl
      cql << " USING TTL ?"
      values.unshift(ttl)
    end
    cql << " SET #{update_cql.join(', ')} WHERE #{key_cql}"
    
    batch_or_execute(cql, values, options)
  end

  # Delete a row from a table. You should pass the primary key value
  # in the key_hash.
  #
  # If this method is called inside a batch block it will be executed in the batch.
  def delete(table, key_hash, options = nil)
    key_cql, key_values = key_clause(key_hash)
    cql = "DELETE FROM #{table} WHERE #{key_cql}"
    batch_or_execute(cql, key_values, options)
  end

  # Execute an arbitrary CQL statment. If values are passed and the statement is a
  # string, it will be prepared and executed as a prepared statement.
  def execute(cql, values = nil, options = nil)
    start_time = Time.now
    begin
      statement = nil
      if cql.is_a?(String)
        if values.present?
          statement = prepare(cql)
        else
          statement = Cassandra::Statements::Simple.new(cql)
        end
      else
        statement = cql
      end
    
      if values.present?
        values = Array(values)
        options = (options ? options.merge(:arguments => values) : {:arguments => values})
      end
    
      # Set a default consistency from a block context if it isn't explicitly set.
      default_consistency = Thread.current[:cassie_consistency]
      if default_consistency
        options = (options ? options.reverse_merge(:consistency => default_consistency) : {:consistency => default_consistency})
      end
      
      session.execute(statement, options || {})
    rescue Cassandra::Errors::IOError => e
      disconnect
      raise e
    ensure
      if statement.is_a?(Cassandra::Statement) && !subscribers.empty?
        payload = Message.new(statement, options, Time.now - start_time)
        subscribers.each{|subscriber| subscriber.call(payload)}
      end
    end
  end

  private
  
  def logger
    self.class.logger
  end

  def session
    connect unless connected?
    @session
  end
  
  def batch_or_execute(cql, values, options = nil)
    batch = Thread.current[:cassie_batch]
    if batch
      batch << [cql, values]
      nil
    else
      execute(cql, values, options)
    end
  end

  def question_marks(size)
    q = '?'
    (size - 1).times{ q << ',?' }
    q
  end

  def key_clause(key_hash)
    cql = []
    values = []
    key_hash.each do |key, value|
      cql << "#{key} = ?"
      values << value
    end
    [cql.join(' AND '), values]
  end
  
  # Extract the CQL from a statement
  def statement_cql(statement, previous = nil)
    cql = nil
    if statement.respond_to?(:cql)
      cql = statement.cql
    elsif statement.respond_to?(:statements) && (previous.nil? || !previous.include?(statement))
      previous ||= []
      previous << statement
      cql = statement.statements.collect{|s| statement_cql(s, previous)}.join('; ')
    end
    cql
  end
end

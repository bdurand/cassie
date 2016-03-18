require 'active_model'
require 'active_support/hash_with_indifferent_access'

# This module provides a simple interface for models backed by Cassandra tables.
#
# Cassandra is very limited in how data can be accessed efficiently so this code
# is intentionally not designed as a full fledged DSL with all the nifty features
# of ActiveRecord. Doing so will only get you into trouble when you run into the
# limits of Cassandra data structures.
#
# It implements ActiveModel::Model and supports ActiveModel callbacks on :create,
# :update, :save, and :destroy as well as ActiveModel validations.
#
# Example:
#
#   class Thing
#     include Cassie::Model
#   
#     self.table_name = "things"
#     self.keyspace = "test"
#     self.primary_key = [:owner, :id]
#   
#     column :owner, :int
#     column :id, :int, :as => :identifier
#     column :val, :varchar, :as => :value
#   
#     ordering_key :id, :desc
#   
#     validates_presence_of :id, :value
#   
#     before_save do
#       ...
#     end
#   end
module Cassie::Model
  extend ActiveSupport::Concern
  include ActiveModel::Model
  include ActiveModel::Validations
  include ActiveModel::Validations::Callbacks
  extend ActiveModel::Callbacks
    
  included do |base|
    class_attribute :table_name, :instance_reader => false, :instance_writer => false
    class_attribute :_keyspace, :instance_reader => false, :instance_writer => false
    class_attribute :_primary_key, :instance_reader => false, :instance_writer => false
    class_attribute :_columns, :instance_reader => false, :instance_writer => false
    class_attribute :_column_aliases, :instance_reader => false, :instance_writer => false
    class_attribute :_ordering_keys, :instance_reader => false, :instance_writer => false
    class_attribute :_counter_table, :instance_reader => false, :instance_writer => false
    define_model_callbacks :create, :update, :save, :destroy
    self._columns = {}
    self._column_aliases = HashWithIndifferentAccess.new
    self._ordering_keys = {}
  end
  
  module ClassMethods    
    # Define a column name and type from the table. Columns must be defined in order
    # to be used. This method will handle defining the getter and setter methods as well.
    #
    # The type specified must be a valid CQL data type.
    #
    # Because Cassandra stores column names with each row it is beneficial to use very short
    # column names. You can specify the :as option to define a more human readable version.
    # This will add the appropriate getter and setter methods as well as allow you to use
    # the alias name in the methods that take an attributes hash.
    #
    # Defining a column will also define getter and setter methods for both the column name
    # and the alias name (if specified). So `column :i, :int, as: :id` will define the methods
    # `i`, `i=`, `id`, and `id=`.
    #
    # If you define a counter column then it will define methods for `increment_i!` and `decrement_i!`
    # which take an optional amount argument. Note that if you have a counter column you cannot have
    # any other non-primary key columns and you cannot call create, update, or save and must use the
    # increment and decrement commands.
    def column(name, type, as: nil)
      name = name.to_sym
      type_class = nil
      begin
        type_class = "Cassandra::Types::#{type.to_s.downcase.classify}".constantize
      rescue NameError
        raise ArgumentError.new("#{type.inspect} is not an allowed Cassandra type")
      end
      
      self._columns = _columns.merge(name => type_class)
      self._column_aliases = self._column_aliases.merge(name => name)

      aliased = (as && as.to_s != name.to_s)
      if aliased
        self._column_aliases = self._column_aliases.merge(as => name)
      end

      if type.to_s == "counter".freeze
        self._counter_table = true
        
        define_method(name){ instance_variable_get(:"@#{name}") || 0 }
        define_method("#{name}="){ |value| instance_variable_set(:"@#{name}", value.to_i) }
        
        define_method("increment_#{name}!"){ |amount=1, ttl: nil| send(:adjust_counter!, name, amount, ttl: ttl) }
        define_method("decrement_#{name}!"){ |amount=1, ttl: nil| send(:adjust_counter!, name, -amount, ttl: ttl) }
        if aliased
          define_method(as){ send(name) }
          define_method("increment_#{as}!"){ |amount=1, ttl: nil| send("increment_#{name}!", amount, ttl: ttl) }
          define_method("decrement_#{as}!"){ |amount=1, ttl: nil| send("increment_#{name}!", amount, ttl: ttl) }
        end
      else
        attr_reader name
        define_method("#{name}="){ |value| instance_variable_set(:"@#{name}", self.class.send(:coerce, value, type_class)) }
        attr_reader name
        if aliased
          define_method(as){ send(name) }
          define_method("#{as}="){|value| send("#{name}=", value) }
        end
      end
    end
    
    # Returns an array of the defined column names as symbols.
    def column_names
      _columns.keys
    end
    
    # Returns the internal column name after resolving any aliases.
    def column_name(name_or_alias)
      name = _column_aliases[name_or_alias] || name_or_alias
    end
    
    # Set the primary key for the table. The value should be set as an array with the
    # clustering key first.
    def primary_key=(value)
      self._primary_key = Array(value).map { |column|
        if column.is_a?(Array)
          column.map(&:to_sym)
        else
          column.to_sym
        end
      }.flatten
    end
    
    # Return an array of column names for the table primary key.
    def primary_key
      _primary_key
    end
    
    # Define and ordering key for the table. The order attribute should be either :asc or :desc
    def ordering_key(name, order)
      order = order.to_sym
      raise ArgumentError.new("order must be either :asc or :desc") unless order == :asc || order == :desc
      _ordering_keys[name.to_sym] = order
    end
  
    # Set the keyspace for the table. The name should be an abstract keyspace name
    # that is mapped to an actual keyspace name in the configuration. If the name
    # provided is not mapped in the configuration, then the raw value will be used.
    def keyspace=(name)
      self._keyspace = name.to_s
    end
    
    # Return the keyspace name where the table is located.
    def keyspace
      connection.config.keyspace(_keyspace)
    end
    
    # Return the full table name including the keyspace.
    def full_table_name
      if _keyspace
        "#{keyspace}.#{table_name}"
      else
        table_name
      end
    end
  
    # Find all records.
    #
    # The +where+ argument can be a Hash, Array, or String WHERE clause to
    # filter the rows returned. It is required so that you don't accidentally
    # release code that returns all rows. If you really want to select all
    # rows from a table you can specify the value :all.
    #
    # The +select+ argument can be used to limit which columns are returned and
    # should be passed as an array of column names which can include aliases.
    #
    # The +order+ argument is a CQL fragment indicating the order. Note that
    # Cassandra will only allow ordering by rows in the primary key.
    #
    # The +limit+ argument specifies how many rows to return.
    #
    # You can provide a block to this method in which case it will yield each
    # record as it is foundto the block instead of returning them.
    def find_all(where:, select: nil, order: nil, limit: nil, options: nil)
      columns = (select ? Array(select).collect{|c| column_name(c)} : column_names)
      cql = "SELECT #{columns.join(', ')} FROM #{full_table_name}"
      values = nil
    
      raise ArgumentError.new("Where clause cannot be blank. Pass :all to find all records.") if where.blank?
      if where && where != :all
        where_clause, values = cql_where_clause(where)
      else
        values = []
      end
      cql << " WHERE #{where_clause}" if where_clause
    
      if order
        cql << " ORDER BY #{order}"
      end
    
      if limit
        cql << " LIMIT ?"
        values << Integer(limit)
      end
    
      results = connection.find(cql, values, options)
      records = [] unless block_given?
      loop do
        results.each do |row|
          record = new(row)
          record.instance_variable_set(:@persisted, true)
          if block_given?
            yield record
          else
            records << record
          end
        end
        break if results.last_page?
        results = results.next_page
      end
      records
    end
    
    # Find a single record that matches the +where+ argument.
    def find(where)
      options = nil
      if where.is_a?(Hash) && where.include?(:options)
        where = where.dup
        options = where.delete(:options)
      end
      find_all(where: where, limit: 1, options: options).first
    end
    
    # Find a single record that matches the +where+ argument or raise an
    # ActiveRecord::RecordNotFound error if none is found.
    def find!(where)
      record = find(where)
      raise Cassie::RecordNotFound unless record
      record
    end
  
    # Return the count of rows in the table. If the +where+ argument is specified
    # then it will be added as the WHERE clause.
    def count(where = nil)
      options = nil
      if where.is_a?(Hash) && where.include?(:options)
        where = where.dup
        options = where.delete(:options)
      end
      
      cql = "SELECT COUNT(*) FROM #{self.full_table_name}"
      values = nil
    
      if where
        where_clause, values = cql_where_clause(where)
        cql << " WHERE #{where_clause}"
      else
        where = connection.prepare(cql)
      end
      
      results = connection.find(cql, values, options)
      results.rows.first["count"]
    end
    
    # Returns a newly created record. If the record is not valid then it won't be
    # persisted.
    def create(attributes)
      record = new(attributes)
      record.save
      record
    end
    
    # Returns a newly created record or raises an ActiveRecord::RecordInvalid error
    # if the record is not valid.
    def create!(attributes)
      record = new(attributes)
      record.save!
      record
    end
    
    # Delete all rows from the table that match the key hash. This method bypasses
    # any destroy callbacks defined on the model.
    def delete_all(key_hash)
      cleanup_up_hash = {}
      key_hash.each do |name, value|
        cleanup_up_hash[column_name(name)] = value
      end
      connection.delete(full_table_name, cleanup_up_hash)
    end
    
    # All insert, update, and delete calls within the block will be sent as a single
    # batch to Cassandra.
    def batch
      connection.batch do
        yield
      end
    end
    
    # Returns the Cassie instance used to communicate with Cassandra.
    def connection
      Cassie.instance
    end
  
    # Since Cassandra doesn't support offset we need to find the order key of record
    # at the specified the offset.
    #
    # The key is a Hash describing the primary keys to search minus the last column defined
    # for the primary key. This column is assumed to be an ordering key. If it isn't, this
    # method will fail.
    #
    # The order argument can be used to specify an order for the ordering key (:asc or :desc).
    # It will default to the natural order of the last ordering key as defined by the ordering_key method.
    #
    # The min and max can be used to limit the offset calculation to a range of values (exclusive).
    def offset_to_id(key, offset, order: nil, batch_size: 1000, min: nil, max: nil)
      ordering_key = primary_key.last
      cluster_order = _ordering_keys[ordering_key] || :asc
      order ||= cluster_order
      order_cql = "#{ordering_key} #{order}" unless order == cluster_order
      
      from = (order == :desc ? max : min)
      to = (order == :desc ? min : max)
      loop do
        limit = (offset > batch_size ? batch_size : offset + 1)
        conditions_cql = []
        conditions = []
        if from
          conditions_cql << "#{ordering_key} #{order == :desc ? '<' : '>'} ?"
          conditions << from
        end
        if to
          conditions_cql << "#{ordering_key} #{order == :desc ? '>' : '<'} ?"
          conditions << to
        end
        key.each do |name, value|
          conditions_cql << "#{column_name(name)} = ?"
          conditions << value
        end
        conditions.unshift(conditions_cql.join(" AND "))

        results = find_all(:select => [ordering_key], :where => conditions, :limit => limit, :order => order_cql)
        last_row = results.last if results.size == limit
        last_id = last_row.send(ordering_key) if last_row
      
        if last_id.nil?
          return nil
        elsif limit >= offset
          return last_id
        else
          offset -= results.size
          from = last_id
        end
      end
    end
  
    private
  
    # Turn a hash of column value, array of [cql, value] or a CQL string into
    # a CQL where clause. Returns the values pulled out in an array for making
    # a prepared statement.
    def cql_where_clause(where)
      case where
      when Hash
        cql = []
        values = []
        where.each do |column, value|
          col_name = column_name(column)
          if value.is_a?(Array)
            q = '?'
            (value.size - 1).times{ q << ',?' }
            cql << "#{col_name} IN (#{q})"
            values.concat(value)
          else
            cql << "#{col_name} = ?"
            values << coerce(value, _columns[col_name])
          end
        end
        [cql.join(' AND '), values]
      when Array
        [where.first, where[1, where.size]]
      when String
        [where, []]
      else
        raise ArgumentError.new("invalid CQL where clause #{where}")
      end
    end
    
    # Force a value to be the correct Cassandra data type.
    def coerce(value, type_class)
      if value.nil?
        nil
      elsif type_class == Cassandra::Types::Timeuuid && value.is_a?(Cassandra::TimeUuid)
        value
      elsif type_class == Cassandra::Types::Uuid
        # Work around for bug in cassandra-driver 2.1.3
        if value.is_a?(Cassandra::Uuid)
          value
        else
          Cassandra::Uuid.new(value)
        end
      elsif type_class == Cassandra::Types::Timestamp && value.is_a?(String)
        Time.parse(value)
      elsif type_class == Cassandra::Types::Inet && value.is_a?(::IPAddr)
        value
      elsif type_class == Cassandra::Types::List
        Array.new(value)
      elsif type_class == Cassandra::Types::Set
        Set.new(value)
      elsif type_class == Cassandra::Types::Map
        Hash[value]
      else
        type_class.new(value)
      end
    end
  end
  
  def initialize(attributes = {})
    super
    @persisted = false
  end
  
  # Return true if the record has been persisted to Cassandra.
  def persisted?
    @persisted
  end
  
  # Return true if the table is used for a counter.
  def counter_table?
    !!self.class._counter_table
  end
  
  # Save a record. Returns true if the record was persisted and false if it was invalid.
  # This method will run the save callbacks as well as either the update or create
  # callbacks as necessary.
  def save(validate: true, ttl: nil)
    raise ArgumentError.new("Cannot call save on a counter table") if counter_table?
    valid_record = (validate ? valid? : true)
    if valid_record
      run_callbacks(:save) do
        if persisted?
          run_callbacks(:update) do
            self.class.connection.update(self.class.full_table_name, values_hash, key_hash, :ttl => (ttl || persistence_ttl))
          end
        else
          run_callbacks(:create) do
            self.class.connection.insert(self.class.full_table_name, attributes, :ttl => (ttl || persistence_ttl))
            @persisted = true
          end
        end
      end
      true
    else
      false
    end
  end
  
  # Save a record. Returns true if the record was saved and raises an ActiveRecord::RecordInvalid
  # error if the record is invalid.
  def save!
    if save
      true
    else
      raise Cassie::RecordInvalid.new(self)
    end
  end
  
  # Delete a record and call the destroy callbacks.
  def destroy
    run_callbacks(:destroy) do
      self.class.connection.delete(self.class.full_table_name, key_hash)
      @persisted = false
      true
    end
  end
  
  # Returns a hash of column to values. Column names will be symbols.
  def attributes
    hash = {}
    self.class.column_names.each do |name|
      hash[name] = send(name)
    end
    hash
  end
  
  # Subclasses can override this method to provide a TTL on the persisted record.
  def persistence_ttl
    nil
  end
  
  def eql?(other)
    other.is_a?(self.class) && other.key_hash == key_hash
  end
  
  def ==(other)
    eql?(other)
  end
  
  # Returns the primary key as a hash
  def key_hash
    hash = {}
    self.class.primary_key.each do |key|
      hash[key] = self.send(key)
    end
    hash
  end
  
  private
  
  # Used for updating counter columns.
  def adjust_counter!(name, amount, ttl: nil)
    amount = amount.to_i
    if amount != 0
      run_callbacks(:update) do
        adjustment = (amount < 0 ? "#{name} = #{name} - #{amount.abs}" : "#{name} = #{name} + #{amount}")
        self.class.connection.update(self.class.full_table_name, adjustment, key_hash, :ttl => (ttl || persistence_ttl))
      end
    end
    record = self.class.find(key_hash)
    value = (record ? record.send(name) : send(name) + amount)
    send("#{name}=", value)
  end
  
  # Returns a hash of value except for the ones that constitute the primary key
  def values_hash
    pk = self.class.primary_key
    hash = {}
    self.class.column_names.each do |name|
      hash[name] = send(name) unless pk.include?(name) 
    end
    hash
  end
end

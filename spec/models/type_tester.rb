require "securerandom"

class Cassie::TypeTester
  include Cassie::Model

  self.table_name = "type_testers"
  self.keyspace = "test"
  self.primary_key = [:id]

  column :id, :varchar
  column :int_value, :int
  column :varint_value, :varint
  column :bigint_value, :bigint
  column :float_value, :float
  column :double_value, :double
  column :decimal_value, :decimal
  column :ascii_value, :ascii
  column :varchar_value, :varchar
  column :text_value, :text
  column :blob_value, :blob
  column :boolean_value, :boolean
  column :timestamp_value, :timestamp
  column :uuid_value, :uuid
  column :timeuuid_value, :timeuuid
  column :inet_value, :inet
  column :list_value, :list
  column :set_value, :set
  column :map_value, :map

  before_create { self.id = SecureRandom.uuid }
end

class Cassie::TypeTesterCounter
  include Cassie::Model

  self.table_name = "type_tester_counters"
  self.keyspace = "test"
  self.primary_key = [:id]

  column :id, :varchar
  column :counter_value, :counter, as: :counter_column
end

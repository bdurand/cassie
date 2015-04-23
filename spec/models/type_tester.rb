class Cassie::TypeTester
  include Cassie::Model
  
  column :int, :int
  column :varint, :varint
  column :bigint, :bigint
  column :float, :float
  column :double, :double
  column :decimal, :decimal
  column :ascii, :ascii
  column :varchar, :varchar
  column :text, :text
  column :blob, :blob
  column :boolean, :boolean
  column :timestamp, :timestamp
  column :counter, :counter
  column :uuid, :uuid
  column :timeuuid, :timeuuid
  column :inet, :inet
  column :list, :list
  column :set, :set
  column :map, :map
end

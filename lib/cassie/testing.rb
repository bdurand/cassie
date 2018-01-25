# This class provides helper methods for testing.
module Cassie::Testing
  extend ActiveSupport::Concern
  
  included do
    prepend OverrideMethods
  end
  
  class << self
    # Prepare the test environment. This method must be called before running the test suite.
    def prepare!
      Cassie.send(:include, Cassie::Testing) unless Cassie.include?(Cassie::Testing)
      Cassie::Schema.all.each do |schema|
        schema.tables.each do |table|
          schema.truncate!(table)
        end
      end
    end
    
    # Wrap test cases as a block in this method. After the test case finishes, all tables
    # that had data inserted into them will be truncated so that the data state will be clean
    # for the next test case.
    def cleanup!
      begin
        yield
      ensure
        if Thread.current[:cassie_inserted].present?
          Cassie.instance.batch do
            Thread.current[:cassie_inserted].each do |table|
              keyspace, table = table.split('.', 2)
              schema = Cassie::Schema.find(keyspace)
              schema.truncate!(table) if schema
            end
          end
          Thread.current[:cassie_inserted] = nil
        end
      end
    end
  end
  
  module OverrideMethods
    def insert(table, *args)
      Thread.current[:cassie_inserted] ||= Set.new
      Thread.current[:cassie_inserted] << table
      super(table, *args)
    end
  end
end

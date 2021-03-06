require 'spec_helper'

describe Cassie do
  
  let(:instance){ Cassie.instance }
  let(:table){ "cassie_specs.things" }
  
  describe "prepare" do
    it "should keep a cache of prepared statements" do
      statement_1 = instance.prepare("SELECT * FROM #{table} LIMIT ?")
      statement_2 = instance.prepare("SELECT * FROM #{table} LIMIT ?")
      statement_1.object_id.should == statement_2.object_id
    end
    
    it "should clear the prepared statement cache when reconnecting" do
      statement_1 = instance.prepare("SELECT * FROM #{table} LIMIT ?")
      instance.disconnect
      instance.connect
      statement_2 = instance.prepare("SELECT * FROM #{table} LIMIT ?")
      statement_1.object_id.should_not == statement_2.object_id
    end
  end
  
  describe "find" do
    before :each do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      instance.insert(table, :owner => 10, :id => 2, :val => 'bar')
    end
    
    it "should construct a CQL query from the options" do
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = 1")
      results.rows.collect{|r| r["val"]}.should == ['foo']
    end
    
    it "should construct a CQL query from a statement with variables" do
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ? LIMIT ?", [10, 1])
      results.rows.collect{|r| r["val"]}.should == ['bar']
    end
    
    it "should not batch find statements" do
      instance.batch do
        results = instance.find("SELECT  owner, id, val FROM #{table} WHERE owner = ? LIMIT ?", [10, 1])
        results.rows.collect{|r| r["val"]}.should == ['bar']
      end
    end
  end
  
  describe "insert" do
    it "should insert a row from a hash of values" do
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
      results.size.should == 0
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
      results.rows.collect{|r| r['val']}.should == ['foo']
    end
    
    it "should add statements to the current batch" do
      instance.batch do
        instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
        results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
        results.size.should == 0
      end
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
      results.size.should == 1
    end
  end
  
  describe "update" do
    it "should update a row from a hash of values and a primary key" do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      instance.update(table, {:val => 'bar'}, :owner => 1, :id => 2)
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
      results.rows.collect{|r| r["val"]}.should == ['bar']
    end

    it "should add statements to the current batch" do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      instance.batch do
        instance.update(table, {:val => 'bar'}, :owner => 1, :id => 2)
        results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
        results.rows.collect{|r| r['val']}.should == ['foo']
      end
      results = instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1)
      results.rows.collect{|r| r['val']}.should == ['bar']
    end
  end
  
  describe "delete" do
    it "should update a row from a primary key hash" do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1).size.should == 1
      instance.delete(table, :owner => 1)
      instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1).size.should == 0
    end

    it "should add statements to the current batch" do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      instance.batch do
        instance.delete(table, :owner => 1)
        instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1).size.should == 1
      end
      instance.find("SELECT owner, id, val FROM #{table} WHERE owner = ?", 1).size.should == 0
    end
  end
  
  describe "execute" do
    before :each do
      instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
    end
    
    it "should execute a plain CQL statement" do
      instance.execute("SELECT owner, id, val FROM #{table} WHERE owner = 1").size.should == 1
    end
    
    it "should execute a prepared statement" do
      statement = instance.prepare("SELECT owner, id, val FROM #{table} WHERE owner = 1")
      instance.execute(statement).size.should == 1
    end
    
    it "should prepare and execute a CQL statement when values are provided" do
      instance.execute("SELECT owner, id, val FROM #{table} WHERE owner = ?", [1]).size.should == 1
    end
    
    it "should call subscribers with details about the call" do
      instance.subscribers.clear
      begin
        messages = []
        instance.subscribers << lambda{|details| messages << details}

        instance.execute("SELECT owner, id, val FROM #{table} WHERE owner = ?", [1])
        messages.size.should == 1
        message = messages.shift
        message.statement.should be_a(Cassandra::Statement)
        message.options.should == {:arguments => [1], :consistency => :local_one}
        message.elapsed_time.should be_a(Float)

        instance.execute("SELECT owner, id, val FROM #{table} WHERE owner = 1")
        messages.size.should == 1
        message = messages.shift
        message.statement.should be_a(Cassandra::Statement)
        message.options.should == {:consistency => :local_one}
        message.elapsed_time.should be_a(Float)

        instance.batch do
          instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
          instance.delete(table, :owner => 1)
        end
        messages.size.should == 1
        message = messages.shift
        message.statement.should be_a(Cassandra::Statements::Batch)
        message.options.should == {:consistency => :local_one}
        message.elapsed_time.should be_a(Float)
      ensure
        instance.subscribers.clear
      end
    end
  end
  
  describe "consistency" do
    let(:session){ instance.send(:session) }
    
    it "should not specify query consistency by default" do
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT * FROM dual"), {:consistency => :local_one})
      instance.execute("SELECT * FROM dual")
      expect(instance.current_consistency).to eq :local_one
    end
    
    it "should allow specifying the consistency in a block" do
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT * FROM dual"), {:consistency => :one})
      Cassie.consistency(:one) do
        instance.execute("SELECT * FROM dual")
        expect(instance.current_consistency).to eq :one
      end
    end
    
    it "should allow specifying the consistency in a block if statement consistency is explicitly nil" do
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT * FROM dual"), {:consistency => :one})
      Cassie.consistency(:one) do
        instance.execute("SELECT * FROM dual", nil, :consistency => nil)
        expect(instance.current_consistency).to eq :one
      end
    end
    
    it "should use the consistency specified to execute if provided" do
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT * FROM dual"), {:consistency => :two})
      Cassie.consistency(:one) do
        instance.execute("SELECT * FROM dual", nil, :consistency => :two)
        expect(instance.current_consistency).to eq :one
      end
    end
    
    it "should use the consistency passed in the batch for all statements in the batch" do
      expect(session).to receive(:execute).with(an_instance_of(Cassandra::Statements::Batch::Logged), {:consistency => :two})
      Cassie.instance.batch(:consistency => :two) do
        instance.insert(table, :owner => 1, :id => 2, :val => 'foo')
      end
    end
    
    it "should be able to specify a global consistancy" do
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT * FROM dual"), {:consistency => :one})
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT COUNT(*) FROM dual"), {:consistency => :local_quorum})
      expect(session).to receive(:execute).with(Cassandra::Statements::Simple.new("SELECT COUNT(1) FROM dual"), {:consistency => :local_one})
      instance.execute("SELECT COUNT(1) FROM dual", nil)
      expect(instance.current_consistency).to eq :local_one
      
      begin
        instance.consistency = :local_quorum
        expect(instance.current_consistency).to eq :local_quorum
        instance.execute("SELECT COUNT(*) FROM dual", nil)
        Cassie.consistency(:one) do
          instance.execute("SELECT * FROM dual", nil)
          expect(instance.current_consistency).to eq :one
        end
        expect(instance.current_consistency).to eq :local_quorum
      ensure
        instance.consistency = :local_one
      end
    end
  end
end

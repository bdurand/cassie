require "spec_helper"

describe Cassie::Model do
  
  describe "definition" do
    it "should define the table name" do
      Cassie::Thing.table_name.should == "things"
      Cassie::Thing.keyspace.should == "cassie_specs"
      Cassie::Thing.full_table_name.should == "cassie_specs.things"
      Cassie::Thing.column_names.should =~ [:owner, :id, :val]
    end
    
    it "should define the primary key" do
      Cassie::Thing.primary_key.should == [:owner, :id]
    end
    
    it "should alias abbreviated column names with human readable names" do
      m = Cassie::Thing.new
      m.id = 1
      m.identifier.should == 1
      m.identifier = 2
      m.id.should == 2
    end
    
    it "should allow null values" do
      Cassie::Thing.create!(:owner => 1, :id => 2)
      record = Cassie::Thing.find(:owner => 1, :id => 2)
      record.value.should == nil
    end
  end
  
  describe "create" do
    it "should create a record" do
      record = Cassie::Thing.create(:owner => 1, :identifier => 2, :value => 'foo')
      record.owner.should == 1
      Cassie::Thing.find(:owner => 1, :identifier => 2).value.should == 'foo'
    end
    
    it "should not save an invalid record" do
      record = Cassie::Thing.create(:owner => 1, :value => 'foo')
      record.should_not be_valid
      Cassie::Thing.count(:owner => 1).should == 0
    end
    
    it "should error on an invalid record using the bang version" do
      lambda{ Cassie::Thing.create!(:owner => 1, :value => 'foo') }.should raise_error(Cassie::RecordInvalid)
    end
  end
  
  describe "delete_all" do
    it "should delete all records matching the key" do
      Cassie::Thing.create(:owner => 1, :id => 2, :val => 'foo')
      Cassie::Thing.create(:owner => 1, :id => 3, :val => 'bar')
      Cassie::Thing.count(:owner => 1).should == 2
      Cassie::Thing.delete_all(:owner => 1, :id => 2)
      Cassie::Thing.count(:owner => 1).should == 1
      Cassie::Thing.find(:owner => 1, :id => 2).should == nil
      Cassie::Thing.find(:owner => 1, :id => 3).should_not == nil
    end
  end
  
  describe "finding" do
    let!(:r1){ Cassie::Thing.create(:owner => 1, :id => 2, :val => 'foo') }
    let!(:r2){ Cassie::Thing.create(:owner => 1, :id => 3, :val => 'bar') }
    let!(:r3){ Cassie::Thing.create(:owner => 2, :id => 3, :val => 'blah') }
    
    it "should find all records using a variety of syntaxes" do
      Cassie::Thing.find_all(where: {:owner => 1}, order: "id ASC").should == [r1, r2]
      Cassie::Thing.find_all(where: {:owner => 1}, order: "id DESC").should == [r2, r1]
      Cassie::Thing.find_all(where: {:owner => 1}, order: "id ASC", limit: 1).should == [r1]
      Cassie::Thing.find_all(where: "owner = 1", order: "id ASC").should == [r1, r2]
      Cassie::Thing.find_all(where: ["owner = ?", 1], order: "id ASC").should == [r1, r2]
      Cassie::Thing.find_all(where: {:owner => 0}, order: "id ASC").should == []
      Cassie::Thing.find_all(where: {:owner => 1, :id => 2}).should == [r1]
      Cassie::Thing.find_all(where: {:owner => [1, 2]}).should =~ [r1, r2, r3]
      Cassie::Thing.find_all(where: {:owner => [1, 2]}, options: {:page_size => 1}).should =~ [r1, r2, r3]
    end
    
    it "should find one record" do
      Cassie::Thing.find(:owner => 1, :id => 2).should == r1
      Cassie::Thing.find(:owner => 1, :id => 3).should == r2
      Cassie::Thing.find(:owner => 1, :id => 0).should == nil
    end
    
    it "should raise an error if the record can't be found and called as find!" do
      Cassie::Thing.find!(:owner => 1, :id => 2).should == r1
      lambda{ Cassie::Thing.find!(:owner => 1, :id => 0) }.should raise_error(Cassie::RecordNotFound)
    end
    
    it "should mark found records as persisted" do
      Cassie::Thing.find(:owner => 1, :id => 2).persisted?.should == true
    end
    
    it "should count records" do
      Cassie::Thing.count(:owner => 1).should == 2
      Cassie::Thing.count(:owner => 1, :id => 2).should == 1
    end
    
    it "won't find all records with a blank where clause" do
      expect{ Cassie::Thing.find_all(where: {}) }.to raise_error(ArgumentError)
      Cassie::Thing.find_all(where: :all).size.should == 3
    end
    
    it "should be able to add subscribers" do
      global = nil
      local = nil
      Cassie::Model.find_subscribers << lambda{|info| global = info.rows}
      Cassie::Thing.find_subscribers << lambda{|info| local = info.rows}
      Cassie::Thing.find_all(where: {:owner => 1}).size.should == 2
      global.should == 2
      local.should == 2
    end
  end
  
  describe "offset_to_id" do
    let!(:r1){ Cassie::Thing.create(:owner => 1, :id => 2, :val => 'foo') }
    let!(:r2){ Cassie::Thing.create(:owner => 1, :id => 3, :val => 'bar') }
    let!(:r3){ Cassie::Thing.create(:owner => 1, :id => 4, :val => 'blah') }
    let!(:r4){ Cassie::Thing.create(:owner => 1, :id => 5, :val => 'mip') }
    let!(:r5){ Cassie::Thing.create(:owner => 2, :id => 2, :val => 'grl') }
    
    it "should calculate the ordering key at a specified offset" do
      Cassie::Thing.offset_to_id({:owner => 1}, 2).should == 3
      Cassie::Thing.offset_to_id({:owner => 1}, 2, order: :asc).should == 4
      Cassie::Thing.offset_to_id({:owner => 1}, 2, batch_size: 1).should == 3
      Cassie::Thing.offset_to_id({:owner => 1}, 3, batch_size: 1).should == 2
      Cassie::Thing.offset_to_id({:owner => 1}, 4, batch_size: 1).should == nil
      Cassie::Thing.offset_to_id({:owner => 1}, 4, batch_size: 100).should == nil
      Cassie::Thing.offset_to_id({:owner => 1}, 1, batch_size: 1, min: 3).should == 4
      Cassie::Thing.offset_to_id({:owner => 1}, 1, order: :desc, batch_size: 1, max: 5).should == 3
    end
  end
  
  describe "batch" do
    it "should delegate to Cassie.batch using the write consistency" do
      Cassie::Thing.connection.should be_a(Cassie)
      expect(Cassie::Thing.connection).to receive(:batch).with(:consistency => :quorum).and_call_original
      Cassie::Thing.batch{}
    end
  end
  
  describe "attributes" do
    it "should get and set attributes" do
      record = Cassie::Thing.new(:owner => 1, :id => 2, :val => 'foo')
      record.attributes.should == {:owner => 1, :id => 2, :val => 'foo'}
    end
 
    it "should get and set attributes using human readable names" do
      record = Cassie::Thing.new(:owner => 1, :identifier => 2, :value => 'foo')
      record.attributes.should == {:owner => 1, :id => 2, :val => 'foo'}
    end
  end
  
  describe "save" do
    it "should not save an invalid record" do
      record = Cassie::Thing.new(:owner => 1, :val => 'foo')
      record.save.should == false
      Cassie::Thing.count(:owner => 1).should == 0
    end
    
    it "should raise an error on the bang version on an invalid record" do
      record = Cassie::Thing.new(:owner => 1, :val => 'foo')
      lambda{ record.save! }.should raise_error(Cassie::RecordInvalid)
      Cassie::Thing.count(:owner => 1).should == 0
    end
    
    it "should save new records and invoke the create callbacks" do
      record = Cassie::Thing.new(:owner => 1, :id => 2, :val => 'foo')
      record.persisted?.should == false
      record.save.should == true
      record.persisted?.should == true
      record.callbacks.should == [:save, :create]
      Cassie::Thing.find(:owner => 1, :id => 2).should == record
    end
    
    it "should save existing records and invoke the update callbacks" do
      Cassie::Thing.create(:owner => 1, :id => 2, :val => 'foo')
      record = Cassie::Thing.find(:owner => 1, :id => 2)
      record.persisted?.should == true
      record.value = 'bar'
      record.save.should == true
      record.persisted?.should == true
      record.callbacks.should == [:save, :update]
      Cassie::Thing.find(:owner => 1, :id => 2).value.should == 'bar'
    end
    
    it "should save new records with a ttl" do
      expect(Cassie::Thing.connection).to receive(:insert).with("cassie_specs.things", {:owner=>1, :id=>2, :val=>'foo'}, {:consistency=>:quorum, :ttl=>10}).and_call_original
      record = Cassie::Thing.new(:owner => 1, :id => 2, :val => 'foo')
      record.persisted?.should == false
      record.save(ttl: 10).should == true
      record.persisted?.should == true
      record.callbacks.should == [:save, :create]
      Cassie::Thing.find(:owner => 1, :id => 2).should == record
    end
  end
  
  describe "destroy" do
    it "should delete a record from Cassandra calling any destroy callbacks" do
      Cassie::Thing.create(:owner => 1, :id => 2, :val => 'foo')
      record = Cassie::Thing.find(:owner => 1, :id => 2)
      record.destroy
      Cassie::Thing.find(:owner => 1, :id => 2).should == nil
      record.callbacks.should =~ [:destroy]
    end
  end
  
  describe "consistency" do
    let(:connection){ Cassie::Thing.connection }

    it "should be able to set a model level read consistency" do
      expect(connection).to receive(:find).with("SELECT owner, id, val FROM cassie_specs.things WHERE owner = ?", [0], {:consistency => :one}).and_call_original
      Cassie::Thing.find_all(where: {:owner => 0})
    end
    
    it "should be able to override the model level read consistency" do
      save_val = Cassie::Thing.read_consistency
      begin
        Cassie::Thing.read_consistency = :quorum
        expect(connection).to receive(:find).with("SELECT owner, id, val FROM cassie_specs.things WHERE owner = ?", [0], {:consistency => :quorum}).and_call_original
        Cassie::Thing.find_all(where: {:owner => 0})
      ensure
        Cassie::Thing.read_consistency = save_val
      end
    end
    
    it "should be able to set a model level write consistency" do
      thing = Cassie::Thing.new(:owner => 1, :id => 2)
      expect(connection).to receive(:insert).with("cassie_specs.things", {:owner=>1, :id=>2, :val=>nil}, {:consistency=>:quorum, :ttl=>nil}).and_call_original
      thing.save
      
      thing.val = "foo"
      expect(connection).to receive(:update).with("cassie_specs.things", {:val=>"foo"}, {:owner=>1, :id=>2}, {:consistency=>:quorum, :ttl=>nil}).and_call_original
      thing.save
      
      expect(connection).to receive(:delete).with("cassie_specs.things", {:owner=>1, :id=>2}, {:consistency=>:quorum}).and_call_original
      thing.destroy
    end
    
    it "should be able to override the model level write consistency" do
      thing = Cassie::Thing.new(:owner => 1, :id => 2)
      thing.write_consistency = :local_quorum
      
      expect(connection).to receive(:insert).with("cassie_specs.things", {:owner=>1, :id=>2, :val=>nil}, {:consistency=>:local_quorum, :ttl=>nil}).and_call_original
      thing.save
      
      thing.val = "foo"
      expect(connection).to receive(:update).with("cassie_specs.things", {:val=>"foo"}, {:owner=>1, :id=>2}, {:consistency=>:local_quorum, :ttl=>nil}).and_call_original
      thing.save
      
      expect(connection).to receive(:delete).with("cassie_specs.things", {:owner=>1, :id=>2}, {:consistency=>:local_quorum}).and_call_original
      thing.destroy
    end
  end
  
  describe "type conversion" do
    let(:model){ Cassie::TypeTester.new }
    
    it "should work with varchar columns" do
      model.varchar_value = "foo"
      model.varchar_value.should == "foo"
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.varchar_value.should == "foo"
      
      model.varchar_value = nil
      model.varchar_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.varchar_value.should == nil
    end
    
    it "should work with ascii columns" do
      model.ascii_value = "foo"
      model.ascii_value.should == "foo"
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.ascii_value.should == "foo"

      model.ascii_value = nil
      model.ascii_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.ascii_value.should == nil
    end
    
    it "should work with text columns" do
      model.text_value = "foo"
      model.text_value.should == "foo"
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.text_value.should == "foo"

      model.text_value = nil
      model.text_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.text_value.should == nil
    end
    
    it "should work with blob columns" do
      model.blob_value = "foo"
      model.blob_value.should == "foo"
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.blob_value.should == "foo"

      model.blob_value = nil
      model.blob_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.blob_value.should == nil
    end
    
    it "should work with int columns" do
      model.int_value = "1"
      model.int_value.should == 1
      model.int_value = 2
      model.int_value.should == 2
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.int_value.should == 2

      model.int_value = nil
      model.int_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.int_value.should == nil
    end
    
    it "should work with bigint columns" do
      model.bigint_value = "1"
      model.bigint_value.should == 1
      model.bigint_value = 2
      model.bigint_value.should == 2
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.bigint_value.should == 2

      model.bigint_value = nil
      model.bigint_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.bigint_value.should == nil
    end
    
    it "should work with varint columns" do
      model.varint_value = "1"
      model.varint_value.should == 1
      model.varint_value = 2
      model.varint_value.should == 2
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.varint_value.should == 2

      model.varint_value = nil
      model.varint_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.varint_value.should == nil
    end
    
    it "should work with float columns" do
      model.float_value = "1.1"
      model.float_value.should == 1.1
      model.float_value = 2.2
      model.float_value.should == 2.2
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.float_value.round(4).should == 2.2

      model.float_value = nil
      model.float_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.float_value.should == nil
    end
    
    it "should work with double columns" do
      model.double_value = "1.1"
      model.double_value.should == 1.1
      model.double_value = 2.2
      model.double_value.should == 2.2
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.double_value.should == 2.2

      model.double_value = nil
      model.double_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.double_value.should == nil
    end
    
    it "should work with decimal columns" do
      model.decimal_value = "1.1"
      model.decimal_value.should == 1.1
      model.decimal_value.should be_a(BigDecimal)
      model.decimal_value = BigDecimal.new("3.3", 2)
      model.decimal_value.should == BigDecimal.new("3.3", 2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.decimal_value.should == BigDecimal.new("3.3", 2)

      model.decimal_value = nil
      model.decimal_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.decimal_value.should == nil
    end
    
    it "should work with timestamp columns" do
      model.timestamp_value = "2015-04-23T15:23:30"
      model.timestamp_value.should == Time.new(2015, 4, 23, 15, 23, 30)
      model.timestamp_value = Time.new(2015, 4, 23, 15, 25, 30)
      model.timestamp_value.should == Time.new(2015, 4, 23, 15, 25, 30)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.timestamp_value.should == Time.new(2015, 4, 23, 15, 25, 30)

      model.timestamp_value = nil
      model.timestamp_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.timestamp_value.should == nil
    end
    
    it "should work with boolean columns" do
      model.boolean_value = true
      model.boolean_value.should == true
      model.boolean_value = false
      model.boolean_value.should == false
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.boolean_value.should == false

      model.boolean_value = nil
      model.boolean_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.boolean_value.should == nil
    end
    
    it "should work with inet columns" do
      model.inet_value = "127.0.0.1"
      model.inet_value.should == IPAddr.new("127.0.0.1")
      model.inet_value = IPAddr.new("10.1.0.1")
      model.inet_value.should == IPAddr.new("10.1.0.1")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.inet_value.should == IPAddr.new("10.1.0.1")
      
      model.inet_value = nil
      model.inet_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.inet_value.should == nil
    end
    
    it "should work with uuid columns" do
      model.uuid_value = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      model.uuid_value.should == Cassandra::Uuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf")
      model.uuid_value = Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.uuid_value.should == Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.uuid_value.should == Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")

      model.uuid_value = nil
      model.uuid_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.uuid_value.should == nil
    end
    
    it "should work with timeuuid columns" do
      model.timeuuid_value = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      model.timeuuid_value.should == Cassandra::TimeUuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf")
      model.timeuuid_value = Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.timeuuid_value.should == Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.timeuuid_value.should == Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")

      model.timeuuid_value = nil
      model.timeuuid_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.timeuuid_value.should == nil
    end
    
    it "should work with list columns" do
      model.list_value = ["a", "b", "c"]
      model.list_value.should == ["a", "b", "c"]
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.list_value.should == ["a", "b", "c"]

      model.list_value = nil
      model.list_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.list_value.should == nil
    end
    
    it "should work with set columns" do
      model.set_value = ["a", "b", "c", "a"]
      model.set_value.should == ["a", "b", "c"].to_set
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.set_value.should == ["a", "b", "c"].to_set

      model.set_value = nil
      model.set_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.set_value.should == nil
    end
    
    it "should work with map columns" do
      model.map_value = [["a", "b"], ["c", "d"]]
      model.map_value.should == {"a" => "b", "c" => "d"}
      model.map_value = {"e" => "f", "g" => "h"}
      model.map_value.should == {"e" => "f", "g" => "h"}
      model.save
      id = model.id
      model = Cassie::TypeTester.find(:id => id)
      model.map_value.should == {"e" => "f", "g" => "h"}

      model.map_value = nil
      model.map_value.should == nil
      model.save
      model = Cassie::TypeTester.find(:id => id)
      model.map_value.should == nil
    end
    
    it "should work with counter columns" do
      id = SecureRandom.uuid
      model = Cassie::TypeTesterCounter.new(:id => id)
      model.counter_value.should == 0
      model.increment_counter_value!
      model.counter_value.should == 1
      model = Cassie::TypeTesterCounter.find(:id => id)
      model.counter_value.should == 1

      model.increment_counter_value!
      model.counter_value.should == 2
      model = Cassie::TypeTesterCounter.find(:id => id)
      model.counter_value.should == 2
      
      model.decrement_counter_value!
      model.counter_value.should == 1
      model = Cassie::TypeTesterCounter.find(:id => id)
      model.counter_value.should == 1
    end
  end
end

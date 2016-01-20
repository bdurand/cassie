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
    it "should delegate to Cassie.batch" do
      Cassie::Thing.connection.should be_a(Cassie)
      expect(Cassie::Thing.connection).to receive(:batch).and_call_original
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
  
  describe "type conversion" do
    let(:model){ Cassie::TypeTester.new }
    
    it "should work with varchar columns" do
      model.varchar = "foo"
      model.varchar.should == "foo"
      model.varchar = nil
      model.varchar.should == nil
    end
    
    it "should work with ascii columns" do
      model.ascii = "foo"
      model.ascii.should == "foo"
      model.ascii = nil
      model.ascii.should == nil
    end
    
    it "should work with text columns" do
      model.text = "foo"
      model.text.should == "foo"
      model.text = nil
      model.text.should == nil
    end
    
    it "should work with blob columns" do
      model.blob = "foo"
      model.blob.should == "foo"
      model.blob = nil
      model.blob.should == nil
    end
    
    it "should work with int columns" do
      model.int = "1"
      model.int.should == 1
      model.int = 2
      model.int.should == 2
      model.int = nil
      model.int.should == nil
    end
    
    it "should work with bigint columns" do
      model.bigint = "1"
      model.bigint.should == 1
      model.bigint = 2
      model.bigint.should == 2
      model.bigint = nil
      model.bigint.should == nil
    end
    
    it "should work with varint columns" do
      model.varint = "1"
      model.varint.should == 1
      model.varint = 2
      model.varint.should == 2
      model.varint = nil
      model.varint.should == nil
    end
    
    it "should work with counter columns" do
      model.counter = "1"
      model.counter.should == 1
      model.counter = 2
      model.counter.should == 2
      model.counter = nil
      model.counter.should == nil
    end
    
    it "should work with float columns" do
      model.float = "1.1"
      model.float.should == 1.1
      model.float = 2.2
      model.float.should == 2.2
      model.float = nil
      model.float.should == nil
    end
    
    it "should work with double columns" do
      model.double = "1.1"
      model.double.should == 1.1
      model.double = 2.2
      model.double.should == 2.2
      model.double = nil
      model.double.should == nil
    end
    
    it "should work with decimal columns" do
      model.decimal = "1.1"
      model.decimal.should == 1.1
      model.decimal.should be_a(BigDecimal)
      model.decimal = BigDecimal.new("3.3", 2)
      model.decimal.should == BigDecimal.new("3.3", 2)
      model.decimal = nil
      model.decimal.should == nil
    end
    
    it "should work with timestamp columns" do
      model.timestamp = "2015-04-23T15:23:30"
      model.timestamp.should == Time.new(2015, 4, 23, 15, 23, 30)
      model.timestamp = Time.new(2015, 4, 23, 15, 25, 30)
      model.timestamp.should == Time.new(2015, 4, 23, 15, 25, 30)
      model.timestamp = nil
      model.timestamp.should == nil
    end
    
    it "should work with boolean columns" do
      model.boolean = true
      model.boolean.should == true
      model.boolean = false
      model.boolean.should == false
      model.boolean = nil
      model.boolean.should == nil
    end
    
    it "should work with inet columns" do
      model.inet = "127.0.0.1"
      model.inet.should == IPAddr.new("127.0.0.1")
      model.inet = IPAddr.new("10.1.0.1")
      model.inet.should == IPAddr.new("10.1.0.1")
      model.inet = nil
      model.inet.should == nil
    end
    
    it "should work with uuid columns" do
      model.uuid = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      model.uuid.should == Cassandra::Uuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf")
      model.uuid = Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.uuid.should == Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.uuid = nil
      model.uuid.should == nil
    end
    
    it "should work with timeuuid columns" do
      model.timeuuid = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      model.timeuuid.should == Cassandra::TimeUuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf")
      model.timeuuid = Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.timeuuid.should == Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      model.timeuuid = nil
      model.timeuuid.should == nil
    end
    
    it "should work with list columns" do
      model.list = ["a", "b", "c"]
      model.list.should == ["a", "b", "c"]
      model.list = nil
      model.list.should == nil
    end
    
    it "should work with set columns" do
      model.set = ["a", "b", "c", "a"]
      model.set.should == ["a", "b", "c"].to_set
      model.set = nil
      model.set.should == nil
    end
    
    it "should work with map columns" do
      model.map = [["a", "b"], ["c", "d"]]
      model.map.should == {"a" => "b", "c" => "d"}
      model.map = {"e" => "f", "g" => "h"}
      model.map.should == {"e" => "f", "g" => "h"}
      model.map = nil
      model.map.should == nil
    end
  end
end

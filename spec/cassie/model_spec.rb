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
  
end

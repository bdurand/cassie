require "spec_helper"

describe Cassie::Model do
  describe "definition" do
    it "should define the table name" do
      expect(Cassie::Thing.table_name).to eq("things")
      expect(Cassie::Thing.keyspace).to eq("cassie_specs")
      expect(Cassie::Thing.full_table_name).to eq("cassie_specs.things")
      expect(Cassie::Thing.column_names).to match_array([:owner, :id, :val])
    end

    it "should define the primary key" do
      expect(Cassie::Thing.primary_key).to eq([:owner, :id])
    end

    it "should alias abbreviated column names with human readable names" do
      m = Cassie::Thing.new
      m.id = 1
      expect(m.identifier).to eq(1)
      m.identifier = 2
      expect(m.id).to eq(2)
    end

    it "should allow null values" do
      Cassie::Thing.create!(owner: 1, id: 2)
      record = Cassie::Thing.find(owner: 1, id: 2)
      expect(record.value).to eq(nil)
    end
  end

  describe "create" do
    it "should create a record" do
      record = Cassie::Thing.create(owner: 1, identifier: 2, value: "foo")
      expect(record.owner).to eq(1)
      expect(Cassie::Thing.find(owner: 1, identifier: 2).value).to eq("foo")
    end

    it "should not save an invalid record" do
      record = Cassie::Thing.create(owner: 1, value: "foo")
      expect(record).not_to be_valid
      expect(Cassie::Thing.count(owner: 1)).to eq(0)
    end

    it "should error on an invalid record using the bang version" do
      expect { Cassie::Thing.create!(owner: 1, value: "foo") }.to raise_error(Cassie::RecordInvalid)
    end
  end

  describe "delete_all" do
    it "should delete all records matching the key" do
      Cassie::Thing.create(owner: 1, id: 2, val: "foo")
      Cassie::Thing.create(owner: 1, id: 3, val: "bar")
      expect(Cassie::Thing.count(owner: 1)).to eq(2)
      Cassie::Thing.delete_all(owner: 1, id: 2)
      expect(Cassie::Thing.count(owner: 1)).to eq(1)
      expect(Cassie::Thing.find(owner: 1, id: 2)).to eq(nil)
      expect(Cassie::Thing.find(owner: 1, id: 3)).not_to eq(nil)
    end
  end

  describe "finding" do
    let!(:r1) { Cassie::Thing.create(owner: 1, id: 2, val: "foo") }
    let!(:r2) { Cassie::Thing.create(owner: 1, id: 3, val: "bar") }
    let!(:r3) { Cassie::Thing.create(owner: 2, id: 3, val: "blah") }

    it "should find all records using a variety of syntaxes" do
      expect(Cassie::Thing.find_all(where: {owner: 1}, order: "id ASC")).to eq([r1, r2])
      expect(Cassie::Thing.find_all(where: {owner: 1}, order: "id DESC")).to eq([r2, r1])
      expect(Cassie::Thing.find_all(where: {owner: 1}, order: "id ASC", limit: 1)).to eq([r1])
      expect(Cassie::Thing.find_all(where: "owner = 1", order: "id ASC")).to eq([r1, r2])
      expect(Cassie::Thing.find_all(where: ["owner = ?", 1], order: "id ASC")).to eq([r1, r2])
      expect(Cassie::Thing.find_all(where: {owner: 0}, order: "id ASC")).to eq([])
      expect(Cassie::Thing.find_all(where: {owner: 1, id: 2})).to eq([r1])
      expect(Cassie::Thing.find_all(where: {owner: [1, 2]})).to match_array([r1, r2, r3])
      expect(Cassie::Thing.find_all(where: {owner: [1, 2]}, options: {page_size: 1})).to match_array([r1, r2, r3])
    end

    it "should find one record" do
      expect(Cassie::Thing.find(owner: 1, id: 2)).to eq(r1)
      expect(Cassie::Thing.find(owner: 1, id: 3)).to eq(r2)
      expect(Cassie::Thing.find(owner: 1, id: 0)).to eq(nil)
    end

    it "should raise an error if the record can't be found and called as find!" do
      expect(Cassie::Thing.find!(owner: 1, id: 2)).to eq(r1)
      expect { Cassie::Thing.find!(owner: 1, id: 0) }.to raise_error(Cassie::RecordNotFound)
    end

    it "should mark found records as persisted" do
      expect(Cassie::Thing.find(owner: 1, id: 2).persisted?).to eq(true)
    end

    it "should count records" do
      expect(Cassie::Thing.count(owner: 1)).to eq(2)
      expect(Cassie::Thing.count(owner: 1, id: 2)).to eq(1)
    end

    it "won't find all records with a blank where clause" do
      expect { Cassie::Thing.find_all(where: {}) }.to raise_error(ArgumentError)
      expect(Cassie::Thing.find_all(where: :all).size).to eq(3)
    end

    it "should be able to add subscribers" do
      global = nil
      local = nil
      Cassie::Model.find_subscribers << lambda { |info| global = info.rows }
      Cassie::Thing.find_subscribers << lambda { |info| local = info.rows }
      expect(Cassie::Thing.find_all(where: {owner: 1}).size).to eq(2)
      expect(global).to eq(2)
      expect(local).to eq(2)
    end
  end

  describe "offset_to_id" do
    let!(:r1) { Cassie::Thing.create(owner: 1, id: 2, val: "foo") }
    let!(:r2) { Cassie::Thing.create(owner: 1, id: 3, val: "bar") }
    let!(:r3) { Cassie::Thing.create(owner: 1, id: 4, val: "blah") }
    let!(:r4) { Cassie::Thing.create(owner: 1, id: 5, val: "mip") }
    let!(:r5) { Cassie::Thing.create(owner: 2, id: 2, val: "grl") }

    it "should calculate the ordering key at a specified offset" do
      expect(Cassie::Thing.offset_to_id({owner: 1}, 2)).to eq(3)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 2, order: :asc)).to eq(4)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 2, batch_size: 1)).to eq(3)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 3, batch_size: 1)).to eq(2)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 4, batch_size: 1)).to eq(nil)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 4, batch_size: 100)).to eq(nil)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 1, batch_size: 1, min: 3)).to eq(4)
      expect(Cassie::Thing.offset_to_id({owner: 1}, 1, order: :desc, batch_size: 1, max: 5)).to eq(3)
    end
  end

  describe "batch" do
    it "should delegate to Cassie.batch using the write consistency" do
      expect(Cassie::Thing.connection).to be_a(Cassie)
      expect(Cassie::Thing.connection).to receive(:batch).with(consistency: :quorum).and_call_original
      Cassie::Thing.batch {}
    end
  end

  describe "attributes" do
    it "should get and set attributes" do
      record = Cassie::Thing.new(owner: 1, id: 2, val: "foo")
      expect(record.attributes).to eq({owner: 1, id: 2, val: "foo"})
      record.attributes = {owner: 2, id: 3, val: "bar"}
      expect(record.attributes).to eq({owner: 2, id: 3, val: "bar"})
    end

    it "should get and set attributes using human readable names" do
      record = Cassie::Thing.new(owner: 1, identifier: 2, value: "foo")
      expect(record.attributes).to eq({owner: 1, id: 2, val: "foo"})
      record.attributes = {owner: 2, identifier: 3, val: "bar"}
      expect(record.attributes).to eq({owner: 2, id: 3, val: "bar"})
    end
  end

  describe "save" do
    it "should not save an invalid record" do
      record = Cassie::Thing.new(owner: 1, val: "foo")
      expect(record.save).to eq(false)
      expect(Cassie::Thing.count(owner: 1)).to eq(0)
    end

    it "should raise an error on the bang version on an invalid record" do
      record = Cassie::Thing.new(owner: 1, val: "foo")
      expect { record.save! }.to raise_error(Cassie::RecordInvalid)
      expect(Cassie::Thing.count(owner: 1)).to eq(0)
    end

    it "should save new records and invoke the create callbacks" do
      record = Cassie::Thing.new(owner: 1, id: 2, val: "foo")
      expect(record.persisted?).to eq(false)
      expect(record.save).to eq(true)
      expect(record.persisted?).to eq(true)
      expect(record.callbacks).to eq([:save, :create])
      expect(Cassie::Thing.find(owner: 1, id: 2)).to eq(record)
    end

    it "should save existing records and invoke the update callbacks" do
      Cassie::Thing.create(owner: 1, id: 2, val: "foo")
      record = Cassie::Thing.find(owner: 1, id: 2)
      expect(record.persisted?).to eq(true)
      record.value = "bar"
      expect(record.save).to eq(true)
      expect(record.persisted?).to eq(true)
      expect(record.callbacks).to eq([:save, :update])
      expect(Cassie::Thing.find(owner: 1, id: 2).value).to eq("bar")
    end

    it "should save new records with a ttl" do
      expect(Cassie::Thing.connection).to receive(:insert).with("cassie_specs.things", {owner: 1, id: 2, val: "foo"}, {consistency: :quorum, ttl: 10}).and_call_original
      record = Cassie::Thing.new(owner: 1, id: 2, val: "foo")
      expect(record.persisted?).to eq(false)
      expect(record.save(ttl: 10)).to eq(true)
      expect(record.persisted?).to eq(true)
      expect(record.callbacks).to eq([:save, :create])
      expect(Cassie::Thing.find(owner: 1, id: 2)).to eq(record)
    end
  end

  describe "update" do
    it "should set attributes and save" do
      record = Cassie::Thing.create(owner: 1, id: 2, val: "foo")
      record.update(owner: 2)
      record.reload
      expect(record.owner).to eq 2
      record.update!(owner: 3)
      record.reload
      expect(record.owner).to eq 3
    end
  end

  describe "primary_key" do
    it "should return the primary key as a hash" do
      record = Cassie::Thing.create(owner: 1, id: 2, val: "foo")
      expect(record.primary_key).to eq({owner: 1, id: 2})
    end
  end

  describe "destroy" do
    it "should delete a record from Cassandra calling any destroy callbacks" do
      Cassie::Thing.create(owner: 1, id: 2, val: "foo")
      record = Cassie::Thing.find(owner: 1, id: 2)
      record.destroy
      expect(Cassie::Thing.find(owner: 1, id: 2)).to eq(nil)
      expect(record.callbacks).to match_array([:destroy])
    end
  end

  describe "consistency" do
    let(:connection) { Cassie::Thing.connection }

    it "should be able to set a model level read consistency" do
      expect(connection).to receive(:find).with("SELECT owner, id, val FROM cassie_specs.things WHERE owner = ?", [0], {consistency: :one}).and_call_original
      Cassie::Thing.find_all(where: {owner: 0})
    end

    it "should be able to override the model level read consistency" do
      save_val = Cassie::Thing.read_consistency
      begin
        Cassie::Thing.read_consistency = :quorum
        expect(connection).to receive(:find).with("SELECT owner, id, val FROM cassie_specs.things WHERE owner = ?", [0], {consistency: :quorum}).and_call_original
        Cassie::Thing.find_all(where: {owner: 0})
      ensure
        Cassie::Thing.read_consistency = save_val
      end
    end

    it "should be able to set a model level write consistency" do
      thing = Cassie::Thing.new(owner: 1, id: 2)
      expect(connection).to receive(:insert).with("cassie_specs.things", {owner: 1, id: 2, val: nil}, {consistency: :quorum, ttl: nil}).and_call_original
      thing.save

      thing.val = "foo"
      expect(connection).to receive(:update).with("cassie_specs.things", {val: "foo"}, {owner: 1, id: 2}, {consistency: :quorum, ttl: nil}).and_call_original
      thing.save

      expect(connection).to receive(:delete).with("cassie_specs.things", {owner: 1, id: 2}, {consistency: :quorum}).and_call_original
      thing.destroy
    end

    it "should be able to override the model level write consistency" do
      thing = Cassie::Thing.new(owner: 1, id: 2)
      thing.write_consistency = :local_quorum

      expect(connection).to receive(:insert).with("cassie_specs.things", {owner: 1, id: 2, val: nil}, {consistency: :local_quorum, ttl: nil}).and_call_original
      thing.save

      thing.val = "foo"
      expect(connection).to receive(:update).with("cassie_specs.things", {val: "foo"}, {owner: 1, id: 2}, {consistency: :local_quorum, ttl: nil}).and_call_original
      thing.save

      expect(connection).to receive(:delete).with("cassie_specs.things", {owner: 1, id: 2}, {consistency: :local_quorum}).and_call_original
      thing.destroy
    end
  end

  describe "type conversion" do
    let(:model) { Cassie::TypeTester.new }

    it "should work with varchar columns" do
      model.varchar_value = "foo"
      expect(model.varchar_value).to eq("foo")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.varchar_value).to eq("foo")

      model.varchar_value = nil
      expect(model.varchar_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.varchar_value).to eq(nil)
    end

    it "should work with ascii columns" do
      model.ascii_value = "foo"
      expect(model.ascii_value).to eq("foo")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.ascii_value).to eq("foo")

      model.ascii_value = nil
      expect(model.ascii_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.ascii_value).to eq(nil)
    end

    it "should work with text columns" do
      model.text_value = "foo"
      expect(model.text_value).to eq("foo")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.text_value).to eq("foo")

      model.text_value = nil
      expect(model.text_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.text_value).to eq(nil)
    end

    it "should work with blob columns" do
      model.blob_value = "foo"
      expect(model.blob_value).to eq("foo")
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.blob_value).to eq("foo")

      model.blob_value = nil
      expect(model.blob_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.blob_value).to eq(nil)
    end

    it "should work with int columns" do
      model.int_value = "1"
      expect(model.int_value).to eq(1)
      model.int_value = 2
      expect(model.int_value).to eq(2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.int_value).to eq(2)

      model.int_value = nil
      expect(model.int_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.int_value).to eq(nil)
    end

    it "should work with bigint columns" do
      model.bigint_value = "1"
      expect(model.bigint_value).to eq(1)
      model.bigint_value = 2
      expect(model.bigint_value).to eq(2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.bigint_value).to eq(2)

      model.bigint_value = nil
      expect(model.bigint_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.bigint_value).to eq(nil)
    end

    it "should work with varint columns" do
      model.varint_value = "1"
      expect(model.varint_value).to eq(1)
      model.varint_value = 2
      expect(model.varint_value).to eq(2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.varint_value).to eq(2)

      model.varint_value = nil
      expect(model.varint_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.varint_value).to eq(nil)
    end

    it "should work with float columns" do
      model.float_value = "1.1"
      expect(model.float_value).to eq(1.1)
      model.float_value = 2.2
      expect(model.float_value).to eq(2.2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.float_value.round(4)).to eq(2.2)

      model.float_value = nil
      expect(model.float_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.float_value).to eq(nil)
    end

    it "should work with double columns" do
      model.double_value = "1.1"
      expect(model.double_value).to eq(1.1)
      model.double_value = 2.2
      expect(model.double_value).to eq(2.2)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.double_value).to eq(2.2)

      model.double_value = nil
      expect(model.double_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.double_value).to eq(nil)
    end

    it "should work with decimal columns" do
      model.decimal_value = "1.1"
      expect(model.decimal_value).to eq(1.1)
      expect(model.decimal_value).to be_a(BigDecimal)
      model.decimal_value = BigDecimal("3.3", 2)
      expect(model.decimal_value).to eq(BigDecimal("3.3", 2))
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.decimal_value).to eq(BigDecimal("3.3", 2))

      model.decimal_value = nil
      expect(model.decimal_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.decimal_value).to eq(nil)
    end

    it "should work with timestamp columns" do
      model.timestamp_value = "2015-04-23T15:23:30"
      expect(model.timestamp_value).to eq(Time.new(2015, 4, 23, 15, 23, 30))
      model.timestamp_value = Time.new(2015, 4, 23, 15, 25, 30)
      expect(model.timestamp_value).to eq(Time.new(2015, 4, 23, 15, 25, 30))
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.timestamp_value).to eq(Time.new(2015, 4, 23, 15, 25, 30))

      model.timestamp_value = nil
      expect(model.timestamp_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.timestamp_value).to eq(nil)
    end

    it "should work with boolean columns" do
      model.boolean_value = true
      expect(model.boolean_value).to eq(true)
      model.boolean_value = false
      expect(model.boolean_value).to eq(false)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.boolean_value).to eq(false)

      model.boolean_value = nil
      expect(model.boolean_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.boolean_value).to eq(nil)
    end

    it "should work with inet columns" do
      model.inet_value = "127.0.0.1"
      expect(model.inet_value).to eq(IPAddr.new("127.0.0.1"))
      model.inet_value = IPAddr.new("10.1.0.1")
      expect(model.inet_value).to eq(IPAddr.new("10.1.0.1"))
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.inet_value).to eq(IPAddr.new("10.1.0.1"))

      model.inet_value = nil
      expect(model.inet_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.inet_value).to eq(nil)
    end

    it "should work with uuid columns" do
      model.uuid_value = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      expect(model.uuid_value).to eq(Cassandra::Uuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf"))
      model.uuid_value = Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      expect(model.uuid_value).to eq(Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf"))
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.uuid_value).to eq(Cassandra::Uuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf"))

      model.uuid_value = nil
      expect(model.uuid_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.uuid_value).to eq(nil)
    end

    it "should work with timeuuid columns" do
      model.timeuuid_value = "eed6d678-ea0b-11e4-8772-793f91a64daf"
      expect(model.timeuuid_value).to eq(Cassandra::TimeUuid.new("eed6d678-ea0b-11e4-8772-793f91a64daf"))
      model.timeuuid_value = Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf")
      expect(model.timeuuid_value).to eq(Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf"))
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.timeuuid_value).to eq(Cassandra::TimeUuid.new("fed6d678-ea0b-11e4-8772-793f91a64daf"))

      model.timeuuid_value = nil
      expect(model.timeuuid_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.timeuuid_value).to eq(nil)
    end

    it "should work with list columns" do
      model.list_value = ["a", "b", "c"]
      expect(model.list_value).to eq(["a", "b", "c"])
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.list_value).to eq(["a", "b", "c"])

      model.list_value = nil
      expect(model.list_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.list_value).to eq(nil)
    end

    it "should work with set columns" do
      model.set_value = ["a", "b", "c", "a"]
      expect(model.set_value).to eq(["a", "b", "c"].to_set)
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.set_value).to eq(["a", "b", "c"].to_set)

      model.set_value = nil
      expect(model.set_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.set_value).to eq(nil)
    end

    it "should work with map columns" do
      model.map_value = [["a", "b"], ["c", "d"]]
      expect(model.map_value).to eq({"a" => "b", "c" => "d"})
      model.map_value = {"e" => "f", "g" => "h"}
      expect(model.map_value).to eq({"e" => "f", "g" => "h"})
      model.save
      id = model.id
      model = Cassie::TypeTester.find(id: id)
      expect(model.map_value).to eq({"e" => "f", "g" => "h"})

      model.map_value = nil
      expect(model.map_value).to eq(nil)
      model.save
      model = Cassie::TypeTester.find(id: id)
      expect(model.map_value).to eq(nil)
    end

    it "should work with counter columns" do
      id = SecureRandom.uuid
      model = Cassie::TypeTesterCounter.new(id: id)
      expect(model.counter_value).to eq(0)
      model.increment_counter_value!
      expect(model.counter_value).to eq(1)
      model = Cassie::TypeTesterCounter.find(id: id)
      expect(model.counter_value).to eq(1)

      model.increment_counter_value!
      expect(model.counter_value).to eq(2)
      model = Cassie::TypeTesterCounter.find(id: id)
      expect(model.counter_value).to eq(2)

      model.decrement_counter_value!
      expect(model.counter_value).to eq(1)
      model = Cassie::TypeTesterCounter.find(id: id)
      expect(model.counter_value).to eq(1)
    end
  end
end

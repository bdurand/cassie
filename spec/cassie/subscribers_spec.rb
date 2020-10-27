require "spec_helper"

describe Cassie::Subscribers do
  it "should be able to add and remove a subscriber" do
    subscribers = Cassie::Subscribers.new
    expect(subscribers.empty?).to eq true
    data_1 = []
    data_2 = []
    subscriber_1 = lambda { |info| data_1 << info }
    subscriber_2 = lambda { |info| data_2 << info }
    subscribers.add(subscriber_1)
    subscribers << subscriber_2
    expect(subscribers.empty?).to eq false
    expect(subscribers.size).to eq 2
    expect(subscribers.include?(subscriber_1)).to eq true
    expect(subscribers.include?(subscriber_2)).to eq true

    subscribers.each { |s| s.call(:payload) }
    expect(data_1).to eq [:payload]
    expect(data_2).to eq [:payload]

    subscribers.remove(subscriber_2)
    expect(subscribers.size).to eq 1
    expect(subscribers.include?(subscriber_1)).to eq true
    expect(subscribers.include?(subscriber_2)).to eq false

    subscribers.each { |s| s.call(:more) }
    expect(data_1).to eq [:payload, :more]
    expect(data_2).to eq [:payload]

    subscribers.delete(subscriber_1)
    expect(subscribers.size).to eq 0
  end

  it "should have a hierarchy of subscribers" do
    subscribers_1 = Cassie::Subscribers.new
    subscribers_2 = Cassie::Subscribers.new(subscribers_1)
    subscribers_3 = Cassie::Subscribers.new(subscribers_1)
    data_1 = []
    data_2 = []
    data_3 = []
    subscribers_1 << lambda { |info| data_1 << info }
    subscribers_2 << lambda { |info| data_2 << info }

    expect(subscribers_1.size).to eq 1
    expect(subscribers_2.size).to eq 2
    expect(subscribers_3.size).to eq 1

    subscribers_1.each { |subscriber| subscriber.call(:payload_1) }
    subscribers_2.each { |subscriber| subscriber.call(:payload_2) }
    subscribers_3.each { |subscriber| subscriber.call(:payload_3) }

    expect(data_1).to eq [:payload_1, :payload_2, :payload_3]
    expect(data_2).to eq [:payload_2]

    subscribers_2.clear
    expect(subscribers_2.size).to eq 1
    subscribers_1.clear
    expect(subscribers_2.size).to eq 0
  end
end

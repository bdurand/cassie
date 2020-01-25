[![Maintainability](https://api.codeclimate.com/v1/badges/129ede20094ea298c687/maintainability)](https://codeclimate.com/github/weheartit/cassie/maintainability)

# Cassie

The short and sweet Cassandra object mapper from [We Heart It](http://weheartit.com/)

## Usage

```ruby
class Thing
  # Your model must include this
  include Cassie::Model

  # Set the table name where the data lives.
  self.table_name = "things"

  # Set the keyspace where the table lives. Keyspaces can be defined abstractly and mapped
  # and mapped in a configuration file. This can allow you to have different keyspace names
  # between different environments and still use the same code.
  self.keyspace = "default"

  # You must defind the primary key. They columns must be listed in the order that they apper
  # in the Cassandra CQL PRIMARY KEY clause defining the table.
  self.primary_key = [:owner, :id]

  # All columns are explicitly defined with their name and data type and an optional
  # alias name.
  column :owner, :int
  column :id, :int, :as => :identifier
  column :val, :varchar, :as => :value

  # The ordering keys should also be defined along with how they are ordered.
  ordering_key :id, :desc

  # You can use all the standard ActiveModel validations.
  validates_presence_of :owner, :id

  # You also get before and after callbacks for create, update, save, and destroy.
  before_save :some_callback_method

  ...
end

Cassie.configure!(:host => "localhost")

Thing.create(:owner => 1, :identifier => 2, :value => "woot")

owner_records = Thing.find_all(where: {:owner => 1})

record = Thing.find(:owner => 1, :identifier => 2)
record.value = "woot"
record.save

record.destroy
```

## Features

To add the Cassie behaviors to your model you just need to incude `Cassie::Model` in your class.

### Configuration

You need to configure Cassie before using it. Cassie can only connect to a single Cassandra cluster. To configure Cassie you need to call the Cassie.configure! method with a hash. The options for the hash can be either strings or symbols.

The options should contain all the options to define your cluster connection (see http://datastax.github.io/ruby-driver/api/#cluster-class_method)

You can also pass the following options:

* :cluster - Options to connect to the cluster. See http://datastax.github.io/ruby-driver/api/#cluster-class_method
* :keyspaces - Map of abstract keyspace names to actual keyspace names. This is provided so that you can have handle to reference keyspaces where the actual keyspace names change from environment to environment.
* :default_keyspace - The default keyspace to be used for the connection. All tables will be assumed to exist in this keyspace. (optional)
* :max_prepared_statements - The limit of the number of prepared statements that will be saved on the client. (default 1000)
* :schema_directory - Path to a directory where the schmea files are kept. This value should only be set in development and testing environments. (optional)

### Explicitly defined data structure

Since all aspects of working with Cassandra tables are very tightly tied to their data structures, we make you explicitly define it in you Ruby objects. That way it's all there where the developer can see it and the code can enforce certain things if it needs to.

At a minimum you need to define the table, keyspace, primary key, and columns. For each column you need to define both the name and data type.

### ActiveModel validations and callbacks

You can use all the standard ActiveModel validation methods on your models. You can also define before or after callbacks for create, update, save, and destroy actions.

Note that one difference between Cassandra and other data stores is that data is only eventually consistent. Some subtle results of this:

1. You won't get an error if you try to create a record with the same primary key twice. Cassandra will simple use the second insert as an update statement.
2. If you perform any queries in your validation logic (e.g. to ensure a value is unique), you really need to use a high consistency level like quorum. If you use a low consistency level, there is a chance that your query can hit a node in the cluster that hasn't been replicated to and your validation could make decisions based on the wrong data.

### Consistency

You can specify the consistency for the connection when it is created in the options. This will default to `:local_one`. You can override the global consistency at runtime by setting a new consistency on the instance `Cassie.instance.consistency = :local_quorum`. This will be the default consistency used if nothing else specifies a consistency.

You can override the default consistency within a block by using the `Cassie.consistency` method. This method will set the default consistency within a block and can be used in instances where you want to force a specific consistency on all queries within a specific scope.

Finally, you can force a specific consistency by specifying `:consistency` in the options to any of the statements that execute a query. This consistency will override any default consistency you've specified. However, if the statement is a write statement and is wrapped in a batch block, the consistency option will have no effect. You can, though, specify the `:consistency` option in the `batch` call to apply a consistency to all statements in the batch.

In `Cassie::Model` classes, you can specify `read_consistency` and `write_consistency` that will be applied to all statements generated by the model. You could use this, for instance, to force all write operations to use `:quorum` while letting all read operations user `:one`. These consistency levels will override any default levels if they are set.

### Prepared statements

For the best Cassandra performance you need to prepare all your CQL statements on the client. Cassie will handle doing that for you where possible.

Cassie will only prepare a statement if you call a method with value parameters. For instance, in the Cassie::Model#find_all method, you can pass the where clause as either a CQL string, a Hash of values, or an Array in the form [CQL, value, value, ...]. If you pass a CQL string, the statement will not be prepared. If you pass a Hash or an Array, the statement will be prepared and cached locally. If you do have a hard coded CQL string that you will execute multiple times, you can pass it to Cassie.prepare.

Examples:

```ruby
Thing.find_all(where: "owner = 1")                 # Will not use prepared statement
Thing.find_all(where: {:owner => 1})               # Will use pepared statement
Thing.find_all(where: ["owner = ?", 1])            # Will use pepared statement
Thing.find_all(where: Cassie.prepare("owner = 1")) # Will use pepared statement
```

The prepared statement cache is capped to 1000 statements by default. For best performance you should ensure that you aren't preparing statements with arbitrary value interpolated into the CQL; otherwise your prepared statement cache will turn over frequently and you'll lose the performance advantages it provides.

```ruby
Thing.find_all(where: ["owner = #{user.id} AND id > ?", id])  # This type of thing is very bad for the statement cache.
```

If necessary you can increase the prepared statement cache in the configuration with the max_prepared_statments option.

### Batches

You can send all insert, update, and delete statements as a batch to Cassandra by wrapping them with a `batch` block:

```ruby
Thing.batch do
  Thing.delete_all(:owner => 1)
  Thing.create(:owner => 1, :identifier => 2, :value => 'foo')
end
```

Cassandra can perform better when sending statements as a batch. Batching also ensures that all the statements are received by the cluster. They won't all be persisted as a unit like in a relational database transaction, but they will all be eventually persisted.

### Support for short column names

Because Cassandra stores the column name with each value, using descriptive column names is a bad idea if you have a lot of data and small column types (see https://issues.apache.org/jira/browse/CASSANDRA-4175). For instance, if you have an integer column to hold user ids the normal thing to do is name it "user_id". However, in Cassandra, this will result in each column using 7 bytes for the name and only 4 bytes for the value. If your table has billions of rows this can add up pretty quickly. As such, it's best to use very short column names. However, this can make your code pretty unreadable.

Cassie solves this problem by allowing you to provide aliases for columns when you define them on your models.

```ruby
class Data
  include Cassie::Model
  ...
  column :u, :int, as: :user_id
  ...
end
```

This will let you use the more description `user_id` instead of `u` almost everywhere within your Ruby code.

* You can initialize records like `Data.new(:user_id => id)`
* You can find records like `Date.find(:user_id => id)`
* You can access the value like `data.user_id`
* You can set the value like `data.user_id = id`
* etc.

The exceptions are:

* If you call `data.attributes` the keys in the returned hash will be :u instead of :user_id
* If you need to query with raw CQL you'll need to use the actual column name instead of the alias

### Schema Definitions

For development and testing environments you should create a directory that defines your Cassandra schema. The organization should be each keyspace should be defined in a file name "#{keyspace}.cql" where keyspace is the abstract name defined for the keyspace in the keyspaces configuration. Setting up schemas is required for using the testing integration (see below) and is very useful for keeping development environments in sync.

### Testing

Cassie has built in support for testing environments to efficiently cleanup data between test cases with the Cassie::Testing module.

To use it with rspec you should add this code to your spec_helper.rb file:

```ruby
  config.before(:suite) do
    Cassie::Schema.all do |keyspace|
      Cassie::Schema.load!(keyspace)
    end
    Cassie::Testing.prepare!
  end

  config.after(:suite) do
    Cassie::Schema.all do |keyspace|
      Cassie::Schema.drop!(keyspace)
    end
  end

  config.around(:each) do |example|
    Cassie::Testing.cleanup! do
      example.run
    end
  end
```

### Using with Rails

If you're using Rails, Cassie will automatically initialize itself with the configuration file found in config/cassie.yml. You can put ERB code into the configuration if desired.

In development and test environments it will look for the schema definitions in db/cassandra.

If you're using a forking web server (i.e. passenger or unicorn) you will need to handle disconnecting and reconnecting the Cassandra connection after forking. For passenger you should include a file in config/initializers/cassie.rb with:

```ruby
if defined?(PhusionPassenger)
  PhusionPassenger.on_event(:starting_worker_process) do |forked|
    if forked
      # Disconnect if already connected so we don't share connections with other processes.
      if Cassie.instance.connected?
        Cassie.instance.disconnect
      end
      Cassie.instance.connect
    end
  end
end
```

You'll need something similar on other web servers. In any case, you'll want to make sure that you call Cassie.instance.connect in an initializer. It can take several seconds to establish the connection so you really want the connection to created before your server starts accepting traffic.

### Instrumentation

You can add instrumentation via subscribers to the `Cassie.instance`. Subscribers must respond to the `call` method and take a single argument which will be a Cassie::Message object which has the `statement`, `options`, and `elapsed_time`.

```
Cassie.instance.subscribers << lambda{|message| logger.warn("CQL: #{message.statement.cql} with #{message.options} took #{message.elapsed_time}s") if message.elapsed_time > 0.5 && message.statement.cql}
```

You can instrument the finding code on your model by adding a find subscriber either to the model or to Cassie::Model. These subscribers take a block which is yielded to with the CQL, arguments, options, elapsed time, and rows returned.
```
Cassie::Model.find_subscribers << lambda{|message| logger.warn("CQL: #{message.cql}; Rows: #{message.rows}; Time: #{message.elapsed_time}")
```

### Limitations

Ruby 2.0 (or compatible) required.

You can only use one Cassandra cluster with Cassie since it only maintains a single connection. You can, however, use multiple keyspaces within the cluster.

Query methods will not gracefully handle querying records by values other than primary keys. Even though Cassandra will let you do this by passing extra options, Cassie doesn't handle it since it just encourages bad practices. If you need to perform such queries you can always send raw CQL to Cassie#execute.

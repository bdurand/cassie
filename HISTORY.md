## 1.3.0

* Add update, update!, primary_key, and reload methods to models.

## 1.2.0

* Update schema queries to support Cassandra 3.x+
* Require Ruby 2.4 or greater

## 1.1.1
* Update dependencies to support Rails 5.

## 1.1.0

* Bump supported version of cassandra-driver to 3.x.

* Add more control over consistency settings.

* Add read and write consistency on model definitions.

## 1.0.7

* Add find_subscribers to Cassie::Model for instrumenting model find calls.

## 1.0.6

* Wrap raw CQL in a Simple statement for code consistency in subscribers.

## 1.0.5

* Add subscribers for instumenting code.

* Remove hardcoded log warnings for long running statements since this can now be better handled with instrumentation.

## 1.0.4

* Set less cryptic error message for invalid records exceptions.

## 1.0.3

* Fix bugs preventing counter and set data types from working on models.

## 1.0.2

* Set cluster logger on the Cassandra cluster if not already set.

## 1.0.1

* Allow finding the offset to the row from a range.

## 1.0.0

* Initial Release

smart-producer
==============
A specialized command line kafka producer with heuristics geared towards working with JSON topics. Can be used as an 
alternative to the `kafka-producer.sh` that is bundled with [Apache Kafka](http://kafka.apache.org/).

**`smart-producer`** is useful if you 
 * have many topics you need to send data to (`kafka-producer.sh` has a rather high startup overhead)
 * have multiple record files per topic
 * have annoyingly long topic names
 * have JSON records that you'd rather keep nicely formatted instead of crammed onto a single line for 
   `kafka-producer.sh`

**`smart-producer`** features two heuristics:
 * Provided with a set of "known" topic names, it **figures out** which **topic** to send records to, **based on record 
   file paths or sub-strings** of the topic names. `smart-producer` will reject ambiguous abbreviations.
 * When reading record files, it **scans for top-level JSON objects** to produce one Kafka record for each object it finds 
   that way. That means you can keep your JSON formatted and you can put comments _between_ records. This is especially
   helpful to give context to test data records.

**`smart-producer`** has one limitation:
 * it can **only process JSON records** 

Getting Started
---------------
`smart-producer` is a Java application and requires JDK 11+.

To use `smart-producer`, you need to supply it with a list of "known" topics. The recommended way to do this, is 
to create an "arguments" file and supply it as `@path/to/the/arguments.txt` to each invocation of `smart-producer`. 
You could create a short wrapper script for each project that you use `smart-producer` with. 

```shell script
# Arguments files to supply "known" topics
cat <<EOF > ./topics
# Company policy requires that topic names follow this pattern:
--known-topic=incoming-yourproject-order-v1-event
--known-topic=incoming-yourproject-user-v1-event
--known-topic=incoming-yourproject-product-v1-event
--known-topic=incoming-yourproject-product-inventory-v1-event
EOF

# Wrapper script that supplies the known topics argument file and sets the broker address
cat <<EOF > ./smart-producer
#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
exec path/to/smart-producer/bin/smart-producer "@$DIR/topics" --broker "${BROKER-localhost:9092}" "$@"
EOF
chmod a+x ./smart-producer
```

### Examples
All the examples assume that your definition of `smart-producer` has a set of "known" topics and a broker address 
defined.
#### Send individual record files
```shell script
# File name unambiguously maps to topic incoming-project-order-v1-event 
./smart-producer orders.json

# File name unambiguously maps to topic incoming-yourproject-product-v1-event 
# (but the `-v1` is required to disambiguate between product and product-inventory)
./smart-producer product-v1.json

# The `users` path component unambiguously matches the `incoming-yourproject-user-v1-event` topic
./smart-producer inc-test-data/users/full-of-unicode-characters.json

# List as many files as necessary; they get imported in the order listed (within each topic)
./smart-producer orders-1.json orders-2.json

# There is no way to "force" a file to be produced to particular topic. 
# The file path MUST unambiguously map to a topic. The following will result in an error.
./smart-producer --topic=incoming-yourproject-order-v1-event records.json  
```

#### Send entire collections of records
```shell script
# Put list of record files into argument files; Records for different topics get produced in parallel
cat <<EOF >all-records
orders.json
product-v1.json
inc-test-data/users/full-of-unicode-characters.json
orders-1.json orders-2.json
EOF
./smart-producer @./all-records

# ... or let good ol' `find` assemble the list of record files for you
./smart-producer $(find . -name '*.json' )

# Restrict collection of record files to certain topics
./smart-producer @all-records --topic order
./smart-producer $(find . -name '*.json' ) --topic user --topic order

# Each topic you list needs to be an unambiguous match
./smart-producer @all-records --topic product-v1 --topic product-inv
```

#### Record files
```shell script
cat <<EOF >users.json
# This file contains test users
{
  "id": "sam",
  "email": "sam@example.com",
  "roles": ["read", "write"]
}

Anything outside of objects gets ignored.
{ 
  "id": "pro", "email": "pro@example.com", 
  "comment": " smart-producer won't get confused by\" escape sequences or } in strings",
  "nested": { "objects": "are also fine" }
}
EOF
./smart-producer ./users.json
```
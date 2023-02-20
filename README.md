# Faraday, the Clojure DynamoDB client [![Build Status](https://github.com/Taoensso/faraday/actions/workflows/ci.yml/badge.svg)](https://github.com/Taoensso/faraday/actions/workflows/ci.yml)

* [API Docs](http://taoensso.github.io/faraday/)
* Leiningen: `[com.taoensso/faraday "1.12.0"]`
* deps.edn: `com.taoensso/faraday {:mvn/version "1.12.0"}`

DynamoDB makes a great companion for Clojure apps that need a **simple, reliable way to persist data, that scales with predictable performance**. Faraday is a small, fast and intuitive DynamoDB client library for Clojure, built around the AWS Java SDK and originally adapted from [Rotary](https://github.com/weavejester/rotary) by James Reeves.

## Why Faraday?
* Small and simple API, with coverage of the most useful DynamoDB features
* Great performance (zero overhead to the official Java SDK)
* Uses [Nippy](https://github.com/ptaoussanis/nippy) for full support of Clojure's rich data types (with compression too)
* The AWS Java SDK for DynamoDB is awkward and verbose
* General purpose AWS SDKs for Clojure such as [Amazonica](https://github.com/mcohen01/amazonica) or [aws-api](https://github.com/cognitect-labs/aws-api) inherit the awkwardness of the AWS SDK when used to interact with DynamoDB

## Getting started

Add Faraday as a dependency to your project and import faraday into your namespace:

```clojure
(ns my-ns
 (:require [taoensso.faraday :as far]))
```

### Preparing a database

#### Option 1 - Run a local DynamoDB instance

First thing is to start a DynamoDB Local instance. Once DynamoDB Local is up and running in your terminal, you should see something like:

```sh
$ docker run -p 8000:8000 amazon/dynamodb-local
Initializing DynamoDB Local with the following configuration:
Port:		8000
InMemory:	true
DbPath:		null
SharedDb:	false
shouldDelayTransientStatuses:	false
CorsParams:	*
```

Then proceed to connecting with your local instance in the next section.

#### Option 2 - Use DynamoDB in the cloud

Make sure you've got an AWS account - note that there's a **free tier** with limited DynamoDB storage and read/write throughput. Next you'll need credentials for an IAM user with read/write access to your DynamoDB tables.

Ready?

### Connecting

```clojure
(def client-opts
  {;;; For DynamoDB Local just use some random strings here, otherwise include your
   ;;; production IAM keys:
   :access-key "<AWS_DYNAMODB_ACCESS_KEY>"
   :secret-key "<AWS_DYNAMODB_SECRET_KEY>"

   ;;; You may optionally override the default endpoint if you'd like to use DynamoDB
   ;;; Local or a different AWS Region (Ref. http://goo.gl/YmV80o), etc.:
   ;; :endpoint "http://localhost:8000"                   ; For DynamoDB Local
   ;; :endpoint "http://dynamodb.eu-west-1.amazonaws.com" ; For EU West 1 AWS region

   ;;; You may optionally provide your own (pre-configured) instance of the Amazon
   ;;; DynamoDB client for Faraday functions to use.
   ;; :client (AmazonDynamoDBClientBuilder/defaultClient)
  })

(far/list-tables client-opts)
=> [] ; That's good, we don't have any tables yet :)
```

Now let's create a table. This is actually one of the more complicated parts of working with DynamoDB since it requires understanding how DynamoDB provisions capacity and how its idiosyncratic [primary keys](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey) work. We can safely ignore the specifics for now.

```clojure
(far/create-table client-opts :my-table
  [:id :n]  ; Primary key named "id", (:n => number type)
  {:throughput {:read 1 :write 1} ; Read & write capacity (units/sec)
   :block? true ; Block thread during table creation
   })

;; Wait a minute for the table to be created... got a sandwich handy?

(far/list-tables client-opts)
=> [:my-table] ; There's our new table!
```

Let's write something to `:my-table` and then fetch it:

```clojure
(far/put-item client-opts
    :my-table
    {:id 0 ; Remember that this is our primary (indexed) key
     :name "Steve" :age 22 :data (far/freeze {:vector    [1 2 3]
                                              :set      #{1 2 3}
                                              :rational (/ 22 7)
                                              ;; ... Any Clojure data goodness
                                              })})

(far/get-item client-opts :my-table {:id 0})
=> {:id 0 :name "Steve" :age 22 :data {:vector [1 2 3] ...}}
```

### Remaining API

DynamoDB gives you tons of power including **secondary indexes**, **conditional writes**, **batch operations**, **atomic counters**, **tuneable read consistency** and more.

Most of this stuff is controlled through optional arguments and is pretty easy to pick up by seeing the relevant [API] docs:

**Tables**: `list-tables`, `describe-table`, `create-table`, `ensure-table`, `update-table`, `delete-table`.

**Items**: `get-item`, `put-item`, `update-item`, `delete-item`.

**Batch items**: `batch-get-item`, `batch-write-item`.

**Querying**: `query`, `scan`, `scan-parallel`.

**Transactions**: `transact-write-items`, `transact-get-items`

You can also check out the [official AWS DynamoDB documentation](http://aws.amazon.com/documentation/dynamodb/), though there's a lot of Java-land complexity that you won't need to deal with when using Faraday. The most useful single doc is probably on the [DynamoDB data model](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html) and the [DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html).

## Development

This project uses [Testcontainers](https://www.testcontainers.org/) to manage starting and stopping a local DynamoDB instance in docker.

Run the tests locally with:

```bash
lein test
```

Or run tests from a REPL like:

```clj
taoensso.faraday.tests.main> (clojure.test/run-tests)
```

To run the entire test suite against all supported versions of Clojure, use:

```bash
lein test-all
```

## Contributions

Please see GitHub issues for bugs, ideas, etc. **Pull requests welcome**. For a general question on usage, try StackOverflow or ask the Faraday users and developers in `#faraday` at clojurians.slack.com.

## License

Distributed under the [EPL v1.0](https://raw.githubusercontent.com/ptaoussanis/faraday/master/LICENSE) (same as Clojure).

Copyright &copy; 2013-2021 [Peter Taoussanis](https://www.taoensso.com/) and contributors.

<a href="https://www.taoensso.com" title="More stuff by @ptaoussanis at www.taoensso.com">
<img src="https://www.taoensso.com/taoensso-open-source.png" alt="Taoensso open-source" width="400"/></a>

**[CHANGELOG]** | [API] | [Donate] | current [Break Version]:

```clojure
[com.taoensso/faraday "1.10.0"] ; see CHANGELOG for details
```

[![Build Status](https://travis-ci.org/Taoensso/faraday.svg?branch=master)](https://travis-ci.org/Taoensso/faraday)

# Faraday

## Clojure DynamoDB client

[DynamoDB] is *awesome* and makes a great companion for Clojure web apps that need a **simple, reliable way to scale with predictable performance and without the usual headaches**.

Faraday was originally adapted from the [Rotary client] by James Reeves.

## Features
 * Small, simple, API: **coverage of the most useful DynamoDB features**
 * **Great performance** (zero overhead to the official Java SDK)
 * Uses [Nippy] for full support of **Clojure's rich data types**

## Getting started

Add the necessary dependency to your project:

```clojure
[com.taoensso/faraday "1.10.0"]
```

And setup your namespace imports:

```clojure
(ns my-ns (:require [taoensso.faraday :as far]))
```

### Preparing a database

#### Option 1 - Run a local DDB instance

First thing is to start a DynamoDB Local instance. Once DynamoDB Local is up and running in your terminal, you should see something like:

```sh
$ lein dynamodb-local
dynamodb-local: Options {:port 6798, :in-memory? true, :db-path /home/.../.clj-dynamodb-local}
dynamodb-local: Started DynamoDB Local
```

Then proceed to connecting with your local instance in the next section.

#### Option 2 - Spin up a cloud DDB instance on AWS

Make sure you've got an [AWS DynamoDB account] - note that there's a **free tier** with limited storage and read+write throughput. Next you'll need credentials for an IAM user with read+write access to your DynamoDB tables (see the **IAM section of your AWS Management Console**).

Ready?

### Connecting

```clojure
(def client-opts
  {;;; For DDB Local just use some random strings here, otherwise include your
   ;;; production IAM keys:
   :access-key "<AWS_DYNAMODB_ACCESS_KEY>"
   :secret-key "<AWS_DYNAMODB_SECRET_KEY>"

   ;;; You may optionally override the default endpoint if you'd like to use DDB
   ;;; Local or a different AWS Region (Ref. http://goo.gl/YmV80o), etc.:
   ;; :endpoint "http://localhost:6798"                   ; For DDB Local
   ;; :endpoint "http://dynamodb.eu-west-1.amazonaws.com" ; For EU West 1 AWS region

   ;;; You may optionally provide your own (pre-configured) instance of the Amazon
   ;;; DynamoDB client for Faraday functions to use.
   ;; :client (AmazonDynamoDBClientBuilder/defaultClient)
  })

(far/list-tables client-opts)
=> [] ; No tables yet :-(
```

Now let's create a table? This is actually one of the more complicated parts of working with DynamoDB since it requires understanding how DynamoDB [provisions capacity] and how its idiosyncratic [primary keys](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey) work. We can safely ignore the specifics for now.

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

Let's write something to `:my-table` and fetch it back:

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

You can also check out the [official AWS DynamoDB documentation] though there's a lot of irrelevant Java-land complexity you won't need to deal with with Faraday. The most useful single doc is probably on the [DynamoDB data model].

## Development

This project uses the [dynamodb-local] Lein plugin to manage downloading, starting and stopping an in-memory DynamoDB instance.

To run all the tests locally, run:

```bash
lein test
```

If you intend to run tests from a repl, you can start a local DynamoDB instance:

```bash
lein dynamodb-local
```

## Contacting me / contributions

Please use the project's [GitHub issues page] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page] for a list of contributors. You'll also find Faraday users and developers in `#faraday` at clojurians.slack.com.

Otherwise, you can reach me at [Taoensso.com]. Happy hacking!

\- [Peter Taoussanis]

## License

Distributed under the [EPL v1.0] \(same as Clojure).
Copyright &copy; 2013-2016 [Peter Taoussanis].

<!--- Standard links -->
[Taoensso.com]: https://www.taoensso.com
[Peter Taoussanis]: https://www.taoensso.com
[@ptaoussanis]: https://www.taoensso.com
[More by @ptaoussanis]: https://www.taoensso.com
[Break Version]: https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md
[Donate]: http://taoensso.com/clojure/backers

<!--- Standard links (repo specific) -->
[CHANGELOG]: https://github.com/ptaoussanis/faraday/releases
[API]: http://taoensso.github.io/faraday/
[GitHub issues page]: https://github.com/ptaoussanis/faraday/issues
[GitHub contributors page]: https://github.com/ptaoussanis/faraday/graphs/contributors
[EPL v1.0]: https://raw.githubusercontent.com/ptaoussanis/faraday/master/LICENSE
[Hero]: https://raw.githubusercontent.com/ptaoussanis/faraday/master/hero.png "Title"

<!--- Unique links -->
[DynamoDB]: http://aws.amazon.com/dynamodb/
[Rotary client]: https://github.com/weavejester/rotary
[contributors]: https://github.com/ptaoussanis/faraday/graphs/contributors
[Nippy]: https://github.com/ptaoussanis/nippy
[instructions from AWS]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
[AWS DynamoDB account]: http://aws.amazon.com/dynamodb/
[provisions capacity]: http://aws.amazon.com/dynamodb/pricing/
[primary keys]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey
[official AWS DynamoDB documentation]: http://aws.amazon.com/documentation/dynamodb/
[DynamoDB data model]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html
[dynamodb-local]: https://github.com/dmcgillen/clj-dynamodb-local

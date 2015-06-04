**[API docs][]** | **[CHANGELOG][]** | [other Clojure libs][] | [Twitter][] | [contact/contrib](#contact--contributing) | current [Break Version][]:

```clojure
[com.taoensso/faraday "1.7.1"] ; Dev, see CHANGELOG for details
```

# Faraday, a Clojure DynamoDB client

[DynamoDB](http://aws.amazon.com/dynamodb/) is *terrific* and makes a great companion for Clojure web apps that need a **simple, highly-reliable way to scale with predictable performance and without the usual headaches**. Seriously, it _rocks_.

Concerned about the costs? They've been [getting](http://goo.gl/qJP5d) [better](http://goo.gl/hCVxY) recently and are actually pretty decent as of May 2013.

Faraday was adapted from James Reaves' [Rotary client](https://github.com/weavejester/rotary). Why adapt? Freedom to experiment rapidly+aggressively without being particularly concerned about backwards compatibility.

## What's in the box™?
  * Small, simple, API: **complete coverage of DynamoDBv2 features**.
  * **Great performance** (zero overhead to the official Java SDK).
  * Uses [Nippy](https://github.com/ptaoussanis/nippy) to **support Clojure's rich data types** and **high-strength encryption**.

## Getting started

DynamoDB's done a fantastic job of hiding (in a good way) a lot of the complexity (in the Rich Hickey sense) that comes with managing large amounts of data. Despite the power at your disposal, the actual API you'll be using is pretty darn simple (especially via Clojure, as usual).

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.taoensso/faraday "1.7.1"] ; project.clj
(ns my-app (:require [taoensso.faraday :as far])) ; ns
```

### Preparing a database

You have a couple of options to try out DynamoDB. 1) Run a Local instance, or 2) spin up an instance on AWS.

#### DynamoDB Local

First thing is to make sure you've got a DynamoDB Local instance up and running. Follow the [instruction from AWS](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html) (don't worry, you basically just download a JAR file and run it) or use `brew install dynamodb-local` if you're on OSX and is using Homebrew.

Once DynamoDB Local is up and running in your terminal, you should see something like...

```sh
$ dynamodb-local
2014-04-30 16:08:51.050:INFO:oejs.Server:jetty-8.1.12.v20130726
2014-04-30 16:08:51.104:INFO:oejs.AbstractConnector:Started SelectChannelConnector@0.0.0.0:8000
```

Then proceed to connecting with your local instance in the next section.

#### Spin up an instance on AWS

Make sure you've got an **[AWS DynamoDB account](http://aws.amazon.com/dynamodb/)** (there's a **free tier** with 100MB of storage and limited read+write throughput). Next you'll need credentials for an IAM user with read+write access to your DynamoDB tables (see the **IAM section of your AWS Management Console**).

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
   ;; :endpoint "http://localhost:8000"                   ; For DDB Local
   ;; :endpoint "http://dynamodb.eu-west-1.amazonaws.com" ; For EU West 1 AWS region
  })

(far/list-tables client-opts)
=> [] ; No tables yet :-(
```

Well that was easy. How about we create a table? (This is actually one of the most complicated parts of working with DynamoDB since it requires understanding how DynamoDB [provisions capacity](http://aws.amazon.com/dynamodb/pricing/) and how its [primary keys](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey) work. Anyway, we can safely ignore the specifics for now).

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

It really couldn't be simpler!

### API

The above example is just scratching the surface obviously. DynamoDB gives you tons of power including **secondary indexes**, **conditional writes**, **batch operations**, **atomic counters**, **tuneable read consistency** and more.

Most of this stuff is controlled through optional arguments and is pretty easy to pick up by **[seeing the appropriate docstrings](http://ptaoussanis.github.io/faraday/)**:

**Tables**: `list-tables`, `describe-table`, `create-table`, `ensure-table`, `update-table`, `delete-table`.

**Items**: `get-item`, `put-item`, `update-item`, `delete-item`.

**Batch items**: `batch-get-item`, `batch-write-item`.

**Querying**: `query`, `scan`, `scan-parallel`.

You can also check out the [official AWS DynamoDB documentation](http://aws.amazon.com/documentation/dynamodb/) though there's a lot of irrelevant Java-land complexity you won't need to deal with with Faraday. The most useful doc is probably on the [DynamoDB data model](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html).

## Performance

Faraday adds negligable overhead to the [official Java AWS SDK](http://aws.amazon.com/sdkforjava/):

![Performance comparison chart](https://github.com/ptaoussanis/faraday/raw/master/benchmarks/chart.png)

## Contact & contributing

`lein start-dev` to get a (headless) development repl that you can connect to with [Cider][] (Emacs) or your IDE.

Please use the project's GitHub [issues page][] for project questions/comments/suggestions/whatever **(pull requests welcome!)**. Am very open to ideas if you have any!

Otherwise reach me (Peter Taoussanis) at [taoensso.com][] or on [Twitter][]. Cheers!

## License

Copyright &copy; 2012-2014 Peter Taoussanis. Distributed under the [Eclipse Public License][], the same as Clojure.


[API docs]: http://ptaoussanis.github.io/faraday/
[CHANGELOG]: https://github.com/ptaoussanis/faraday/releases
[other Clojure libs]: https://www.taoensso.com/clojure
[taoensso.com]: https://www.taoensso.com
[Twitter]: https://twitter.com/ptaoussanis
[issues page]: https://github.com/ptaoussanis/faraday/issues
[commit history]: https://github.com/ptaoussanis/faraday/commits/master
[Break Version]: https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md
[Leiningen]: http://leiningen.org/
[Cider]: https://github.com/clojure-emacs/cider
[CDS]: http://clojure-doc.org/
[ClojureWerkz]: http://clojurewerkz.org/
[Eclipse Public License]: https://raw2.github.com/ptaoussanis/faraday/master/LICENSE

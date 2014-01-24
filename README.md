**[API docs](http://ptaoussanis.github.io/faraday/)** | **[CHANGELOG](https://github.com/ptaoussanis/faraday/blob/master/CHANGELOG.md)** | [contact & contributing](#contact--contributing) | [other Clojure libs](https://www.taoensso.com/clojure-libraries) | [Twitter](https://twitter.com/#!/ptaoussanis) | current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/faraday "1.0.2"] ; Stable
```

# Faraday, a Clojure DynamoDB client

[DynamoDB](http://aws.amazon.com/dynamodb/) is *terrific* and makes a great companion for Clojure web apps that need a **simple, highly-reliable way to scale with predictable performance and without the usual headaches**. Seriously, it _rocks_.

Concerned about the costs? They've been [getting](http://goo.gl/qJP5d) [better](http://goo.gl/hCVxY) recently and are actually pretty decent as of May 2013.

Faraday was adapted from James Reaves' [Rotary client](https://github.com/weavejester/rotary). Why adapt? Freedom to experiment rapidly+aggresively without being particularly concerned about backwards compatibility.

## What's in the box™?
  * Small, simple, API: **complete coverage of DynamoDBv2 features**.
  * **Great performance** (zero overhead to the official Java SDK).
  * Uses [Nippy](https://github.com/ptaoussanis/nippy) to **support Clojure's rich data types** and **high-strength encryption**.

## Getting started

DynamoDB's done a fantastic job of hiding (in a good way) a lot of the complexity (in the Rich Hickey sense) that comes with managing large amounts of data. Despite the power at your disposal, the actual API you'll be using is pretty darn simple (especially via Clojure, as usual).

### Dependencies

Add the necessary dependency to your [Leiningen](http://leiningen.org/) `project.clj` and `require` the library in your ns:

```clojure
[com.taoensso/faraday "1.0.2"] ; project.clj
(ns my-app (:require [taoensso.faraday :as far])) ; ns
```

### Preparing a database

First thing is to make sure you've got an **[AWS DynamoDB account](http://aws.amazon.com/dynamodb/)** (there's a **free tier** with 100MB of storage and limited read+write throughput). Next you'll need credentials for an IAM user with read+write access to your DynamoDB tables (see the **IAM section of your AWS Management Console**). Ready?

### Connecting

```clojure
(def creds {:access-key "<AWS_DYNAMODB_ACCESS_KEY>"
            :secret-key "<AWS_DYNAMODB_SECRET_KEY>"}) ; Insert your IAM creds here

(far/list-tables creds)
=> [] ; No tables yet :-(
```

Well that was easy. How about we create a table? (This is actually one of the most complicated parts of working with DynamoDB since it requires understanding how DynamoDB [provisions capacity](http://aws.amazon.com/dynamodb/pricing/) and how its [primary keys](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey) work. Anyway, we can safely ignore the specifics for now).

```clojure
(far/create-table creds :my-table
  [:id :n]  ; Primary key named "id", (:n => number type)
  {:throughput {:read 1 :write 1} ; Read & write capacity (units/sec)
   :block? true ; Block thread during table creation
   })

;; Wait a minute for the table to be created... got a sandwich handy?

(far/list-tables creds)
=> [:my-table] ; There's our new table!
```

Let's write something to `:my-table` and fetch it back:

```clojure
(far/put-item creds
    :my-table
    {:id 0 ; Remember that this is our primary (indexed) key
     :name "Steve" :age 22 :data (far/freeze {:vector    [1 2 3]
                                              :set      #{1 2 3}
                                              :rational (/ 22 7)
                                              ;; ... Any Clojure data goodness
                                              })})

(far/get-item creds :my-table {:id 0})
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

You can also check out the [official AWS DynamoDB documentation](http://aws.amazon.com/documentation/dynamodb/) though there's a lot of irrelevant Java-land complexity you won't need to deal with with Farady. The most useful doc is probably on the [DynamoDB data model](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html).

## Performance (TODO)

Faraday adds negligable overhead to the [official Java AWS SDK](http://aws.amazon.com/sdkforjava/):

![Performance comparison chart](https://github.com/ptaoussanis/faraday/raw/master/benchmarks/chart.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs.

## This project supports the CDS and ![ClojureWerkz](https://raw.github.com/clojurewerkz/clojurewerkz.org/master/assets/images/logos/clojurewerkz_long_h_50.png) goals

  * [CDS](http://clojure-doc.org/), the **Clojure Documentation Site**, is a **contributer-friendly** community project aimed at producing top-notch, **beginner-friendly** Clojure tutorials and documentation. Awesome resource.

  * [ClojureWerkz](http://clojurewerkz.org/) is a growing collection of open-source, **batteries-included Clojure libraries** that emphasise modern targets, great documentation, and thorough testing. They've got a ton of great stuff, check 'em out!

## Contact & contributing

Please use the [project's GitHub issues page](https://github.com/ptaoussanis/faraday/issues) for project questions/comments/suggestions/whatever **(pull requests welcome!)**. Am very open to ideas if you have any!

Otherwise reach me (Peter Taoussanis) at [taoensso.com](https://www.taoensso.com) or on Twitter ([@ptaoussanis](https://twitter.com/#!/ptaoussanis)). Cheers!

## License

Copyright &copy; 2013 Peter Taoussanis. Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.

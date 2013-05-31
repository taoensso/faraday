Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/faraday "0.2.0"] ; Alpha - likely buggy, API subject to change
```

# Faraday, a Clojure DynamoDB client

[DynamoDB](http://aws.amazon.com/dynamodb/) is *terrific* and makes a great companion for Clojure web apps that need a **simple, highly-reliable way to scale with predictable performance and without the usual headaches**. Seriously, it rocks.

Concerned about the costs? They've been [getting](http://goo.gl/qJP5d) [better](http://goo.gl/hCVxY) recently and are actually pretty decent as of May 2013.

Faraday is a fork of [Rotary](https://github.com/weavejester/rotary) by James Reaves. Why fork? Freedom to experiment rapidly+aggresively without being particularly concerned about backwards compatibility.

## What's In The Box?
 * Small, simple, **up-to-date API** (pull requests welcome!).
 * **Good performance**.
 * Flexible, high-performance **binary-safe serialization** using [Nippy](https://github.com/ptaoussanis/nippy).

It's still early days. There's rough edges, but most of them should be relatively superficial and will be ironed out as the lib sees Real-World-Use™. Again, since I'm still only evaluating DynamoDB myself: **pull requests, bug reports, and/or suggestions are very, very welcome**!

## Getting Started

DynamoDB's done a fantastic job of hiding (in a good way) a lot of the complexity (in the Rich Hickey sense) that comes with managing large amounts of data. Despite the power at your disposal, the actual API you'll be using is pretty darn simple (especially via Clojure, as usual).

### Leiningen

Depend on Faraday in your `project.clj`:

```clojure
[com.taoensso/faraday "0.2.0"] ; Requires Clojure 1.5+
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.faraday :as far]))
```

### Preparing A Database

First thing is to make sure you've got an **[AWS DynamoDB account](http://aws.amazon.com/dynamodb/)** (there's a **free tier** with 100MB of storage and limited read+write throughput). Next you'll need credentials for an IAM user with read+write access to your DynamoDB tables (see the **IAM section of your AWS Management Console**). Ready?

### Connecting

```clojure
(def my-creds {:access-key "<AWS_DYNAMODB_ACCESS_KEY>"
               :secret-key "<AWS_DYNAMODB_SECRET_KEY>"})

(far/list-tables my-creds)
=> [] ; No tables yet...
```

Well that was easy. How about we create a table? (This is actually one of the most complicated parts of working with DynamoDB since it requires understanding how DynamoDB [provisions capacity](http://aws.amazon.com/dynamodb/pricing/) and how its [primary keys](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey) work. Anyway, we can safely ignore the specific for now).

```clojure
(far/create-table my-creds
  {:name :my-table
   :throughput {:read 1 :write 1}    ; Read & write capacity (units/sec)
   :hash-key   {:name :id :type :n}} ; Primary key (:n => number type)
  )

;; Wait a minute for the table to be created... maybe have a nice sandwich?

(far/list-tables my-creds)
=> [:my-table] ; There's our new table!
```

Let's write something to `:my-table` and fetch it back:

```clojure
(far/put-item my-creds
    :my-table
    {:id 0 ; Remember that this is our primary (indexed) key
     :name "Steve" :age 22 :data (far/freeze {:vector    [1 2 3]
                                              :set      #{1 2 3}
                                              :rational (/ 22 7)
                                              ;; ... Other Clojure data goodness
                                              })})

(far/get-item my-creds :my-table {:id 0})
=> {:id 0 :name "Steve" :age 22 :data {:vector [1 2 3] ...}}
```

It really couldn't be simpler!

### API

The above example is just scratching the surface obviously. DynamoDB gives you tons of power including **secondary indexes**, **conditional writes**, **batch operations**, **atomic counters**, **tuneable read consistency** and more.

Most of this stuff is controlled through optional arguments and is pretty easy to pick up by **seeing the appropriate docstrings**:

**Tables**: `list-tables`, `describe-table`, `create-table`, `ensure-table`, `update-table`, `delete-table`.

**Items**: `get-item`, `put-item`, `update-item`, `delete-item`.

**Batch items**: `batch-get-item`, `batch-write-item`.

**Querying**: `scan`, `query`.

You can also check out the [official AWS DynamoDB documentation](http://aws.amazon.com/documentation/dynamodb/) though there's a lot of irrelevant Java-land complexity you won't need to deal with with Farady.

## TODO Performance

Faraday adds negligable overhead to the [official Java AWS SDK](http://aws.amazon.com/sdkforjava/):

![Performance comparison chart](https://github.com/ptaoussanis/faraday/raw/master/benchmarks/chart.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs.

## Faraday Supports the ClojureWerkz and CDS Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

CDS (Clojure Documentation Site) is a contributor-friendly community project aimed at producing top-notch [Clojure tutorials](http://clojure-doc.org/) and documentation.

## Contact & Contribution

Reach me (Peter Taoussanis) at [taoensso.com](https://www.taoensso.com) for questions/comments/suggestions/whatever. I'm very open to ideas if you have any! I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2013 Peter Taoussanis. Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
<a href="https://www.taoensso.com" title="More stuff by @ptaoussanis at www.taoensso.com">
<img src="https://www.taoensso.com/taoensso-open-source.png" alt="Taoensso open-source" width="400"/></a>

**[CHANGELOG]** | [API] | current [Break Version]:

```clojure
[com.taoensso/faraday "1.9.0"] ; BREAKING, see CHANGELOG for details
```

[![build status](https://gitlab.com/ricardojmendez/faraday/badges/master/build.svg)](https://gitlab.com/ricardojmendez/faraday/commits/master)


> Please consider helping to [support my continued open-source Clojure/Script work]? 
> 
> Even small contributions can add up + make a big difference to help sustain my time writing, maintaining, and supporting Faraday and other Clojure/Script libraries. **Thank you!**
>
> \- Peter Taoussanis

# Faraday

## Clojure DynamoDB client

[DynamoDB] is *awesome* and makes a great companion for Clojure web apps that need a **simple, reliable way to scale with predictable performance and without the usual headaches**.

Faraday was originally adapted from the [Rotary client] by James Reeves.

## Library status

I'm not currently using DDB or Faraday myself but will make a best effort to continue maintaining the library as I can.

The bulk of recent development work has been thanks to the generosity of Faraday's [contributors]! 

PRs for fixes and/or new features **very welcome**!

\- [Peter Taoussanis]

## Features
 * Small, simple, API: **complete coverage of DynamoDBv2 features**
 * **Great performance** (zero overhead to the official Java SDK)
 * Uses [Nippy] for full support of **Clojure's rich data types**

## 3rd-party stuff

Link                     | Description
------------------------ | -----------------------------------------------------
[@mixradio/faraday-atom] | Atom implementation for Faraday
[@ricardojmendez/ddb-tutorial] | **Tutorial**: Clojure and DDB with Faraday
Your link here?          | **PR's welcome!**

## Getting started

> See also [@ricardojmendez/ddb-tutorial] for a full tutorial!

Add the necessary dependency to your project:

```clojure
[com.taoensso/faraday "1.9.0"]
```

And setup your namespace imports:

```clojure
(ns my-ns (:require [taoensso.faraday :as far]))
```

### Preparing a database

#### Option 1 - Run a local DDB instance

First thing is to make sure you've got a DynamoDB Local instance up and running. Follow the [instructions from AWS][] (don't worry, you basically just download a JAR file and run it) or use `brew install dynamodb-local` if you're on OSX and is using Homebrew.

Once DynamoDB Local is up and running in your terminal, you should see something like:

```sh
$ dynamodb-local
2014-04-30 16:08:51.050:INFO:oejs.Server:jetty-8.1.12.v20130726
2014-04-30 16:08:51.104:INFO:oejs.AbstractConnector:Started SelectChannelConnector@0.0.0.0:8000
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
   ;; :endpoint "http://localhost:8000"                   ; For DDB Local
   ;; :endpoint "http://dynamodb.eu-west-1.amazonaws.com" ; For EU West 1 AWS region
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

## Contacting me / contributions

Please use the project's [GitHub issues page] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page] for a list of contributors.

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
[support my continued open-source Clojure/Script work]: http://taoensso.com/clojure/backers

<!--- Standard links (repo specific) -->
[CHANGELOG]: https://github.com/ptaoussanis/faraday/releases
[API]: http://ptaoussanis.github.io/faraday/
[GitHub issues page]: https://github.com/ptaoussanis/faraday/issues
[GitHub contributors page]: https://github.com/ptaoussanis/faraday/graphs/contributors
[EPL v1.0]: https://raw.githubusercontent.com/ptaoussanis/faraday/master/LICENSE
[Hero]: https://raw.githubusercontent.com/ptaoussanis/faraday/master/hero.png "Title"

<!--- Unique links -->

[DynamoDB]: http://aws.amazon.com/dynamodb/
[Rotary client]: https://github.com/weavejester/rotary
[contributors]: https://github.com/ptaoussanis/faraday/graphs/contributors
[Nippy]: https://github.com/ptaoussanis/nippy
[@mixradio/faraday-atom]: https://github.com/mixradio/faraday-atom
[@ricardojmendez/ddb-tutorial]: http://numergent.com/2016-01/Clojure-and-DynamoDB-with-Faraday-part-1.html
[instructions from AWS]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
[AWS DynamoDB account]: http://aws.amazon.com/dynamodb/
[provisions capacity]: http://aws.amazon.com/dynamodb/pricing/
[primary keys]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelPrimaryKey
[official AWS DynamoDB documentation]: http://aws.amazon.com/documentation/dynamodb/
[DynamoDB data model]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html

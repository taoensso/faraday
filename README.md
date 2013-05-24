Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/faraday "0.0.2-SNAPSHOT"] ; WARNING: Still VERY experimental!
```

# Faraday, a Clojure DynamoDB client

[DynamoDB](http://aws.amazon.com/dynamodb/) is *terrific* and makes a great companion for Clojure web apps that need a **simple, highly-reliable way to scale with predictable performance and without the usual headaches**. Seriously, it rocks.

Faraday is a fork of [Rotary](https://github.com/weavejester/rotary) by James Reaves. Why fork? Freedom to experiment rapidly+aggresively without being particularly concerned about backwards compatibility.

## What's In The Box?
 * Small, simple, **up-to-date API** (pull requests welcome!).
 * **Good performance**.
 * Flexible, high-performance **binary-safe serialization** using [Nippy](https://github.com/ptaoussanis/nippy).

## Getting Started

### Leiningen

Depend on Faraday in your `project.clj`:

```clojure
[com.taoensso/faraday "0.0.2-SNAPSHOT"]
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.faraday :as far]))
```

## TODO Documentation

## TODO Performance

![Performance comparison chart](https://github.com/ptaoussanis/faraday/raw/master/benchmarks/chart.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/TODO) is available on Google Docs.

## Faraday Supports the ClojureWerkz and CDS Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

CDS (Clojure Documentation Site) is a contributor-friendly community project aimed at producing top-notch [Clojure tutorials](http://clojure-doc.org/) and documentation.

## Contact & Contribution

Reach me (Peter Taoussanis) at [taoensso.com](https://www.taoensso.com) for questions/comments/suggestions/whatever. I'm very open to ideas if you have any! I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2013 Peter Taoussanis. Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.

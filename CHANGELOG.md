This project uses Break Versioning (https://www.taoensso.com/break-versioning)

## v1.12.2 / 2024 Jun 1

```clojure
[com.taoensso/faraday "1.12.3"]
```

* **Change**: Allow multiple GSIs to be updated via `update-table` (now matches `create-table`).
* **Change**: Upgrade Nippy (see [GHSA-vw78-267v-588h](https://github.com/taoensso/nippy/security/advisories/GHSA-vw78-267v-588h) and [CVE-2024-36114](https://nvd.nist.gov/vuln/detail/CVE-2024-36114)).

## v1.12.2 / 2024 Apr 3

```clojure
[com.taoensso/faraday "1.12.2"]
```

* **Change**: Upgrade dependencies, including upgrade to com.amazonaws/aws-java-sdk-dynamodb 1.12.693 to eliminate software.amazon.ion:ion-java@1.0.2 from transitive dependencies (see [CVE-2024-21634](https://nvd.nist.gov/vuln/detail/CVE-2024-21634) and [#169](https://github.com/Taoensso/faraday/issues/169)).

## v1.12.0 / 2023 Feb 20

```clojure
[com.taoensso/faraday "1.12.0"]
```

* **BREAKING**: Drop support for Clojure 1.5 and 1.6 - Clojure 1.7+ now required.
* **Change** `clj-item->db-item` no longer captures serialize function when namespace is loaded (`extend-protocol` can now be used to change serialize behaviour at any time).
* **Change**: Upgrade dependencies, including upgrade to com.amazonaws/aws-java-sdk-dynamodb 1.12.410.

## v1.11.4 / 2021 Dec 10

```clojure
[com.taoensso/faraday "1.11.4"]
```

* **Fix** Remove dev dependencies from release (see [technomancy/leiningen#2721](https://github.com/technomancy/leiningen/issues/2721))


## v1.11.3 / 2021 Dec 7

```clojure
[com.taoensso/faraday "1.11.3"]
```

* **Fix** Allow string table names in `query` and `scan`

## v1.11.2 / 2021 Jul 24

```clojure
[com.taoensso/faraday "1.11.2"]
```

* **New** `client-opts` can now include `:protocol` (`:HTTP` or `:HTTPS`)
* **Fix** `client-opts` parsing of `:region` is fixed

## v1.11.1 / 2020 Jul 19

```clojure
[com.taoensso/faraday "1.11.1"]
```

* **Change** `batch-get-item`, will no longer stitch requests together unless `span-reqs` is specified. To reinstate the old behaviour, pass `:span-reqs {:max 5}` in the options. See [#74](https://github.com/Taoensso/faraday/issues/74) and [#143](https://github.com/Taoensso/faraday/pull/143).

## v1.11.0 / 2020 Jul 19

```clojure
[com.taoensso/faraday "1.11.0"]
```

* **New** `transact-get-items` and `transact-write-items` for transaction support
* **New** `describe-ttl`, `update-ttl` and `ensure-ttl` to manage table TTL configuration
* **New** `client-opts` may now specify `:region` as an alternative to `:endpoint`
* **Change** `query`, `scan`, `batch-write-item`, will no longer stitch requests together unless `span-reqs` is specified. To reinstate the old behaviour, pass `:span-reqs {:max 5}` in the options. See [#74](https://github.com/Taoensso/faraday/issues/74) and [#143](https://github.com/Taoensso/faraday/pull/143).
* **Change** `update-table` now uses current table description to validate GSI throughput updates
* **Fix** `describe-table` now returns `:billing-mode :provisioned` instead of `:billing-mode nil`

## v1.10.1 / 2019 Nov 18

```clojure
[com.taoensso/faraday "1.10.1"]
```
* **New** `put-item` and other fns now support lazy seqs, as long as they have been realized
* **Fix** `update-table` should not require that `throughput` is given for GSIs on-demand tables

## v1.10.0 / 2019 Nov 13

```clojure
[com.taoensso/faraday "1.10.0"]
```
* **New** `client-opts` can now include `:client` to specify a custom, pre-configured AmazonDynamoDBClient instance
* **New** `create-table` and `update-table` now support `:billing-mode` of `:provisioned` (default) or `:pay-per-request` (on-demand)
* **New** Accept keywords for index names in `scan` and `query`
* **New** `create-table` and `update-table` now support `:stream-spec` to activate DynamoDB Streams
* **New** `list-streams`, `describe-stream`, `shard-iterator` and `get-stream-records` added to interact with DynamoDB Streams
* **New** `CljVal->DbVal` protocol for extensible serialisation
* **Change**: Upgrade to com.amazonaws/aws-java-sdk-dynamodb 1.11.x
* **Change**: Migrate from expectations to clojure.test
* **Fix**: Fix syntax quote for `without-attr-multi-vs`
* **Fix**: Fix 'non-expand' mode `batch-write-item`
* **Fix**: Fix merging of pages in `batch-get-item`

## v1.9.0 / 2016 Jul 24

```clojure
[com.taoensso/faraday "1.9.0"]
```

> This is a major feature release with **BREAKING CHANGES** (see **Migration** section for details).
> Big thanks to @ricardojmendez for most of the work for this release!

* **BREAKING**: `update-item` args have changed (`:update-map` is now optional) [@ricardojmendez] **[1]**
* **BREAKING**: `update-table` args have changed (`:throughput` is now optional) [@ricardojmendez] **[2]**
* **New**: `put-item`, `update-item` support for expressions [@leonardoborges #73]
* **New**: `get-item` support for `:proj-expr`, `:expr-names` [@ricardojmendez]
* **New**: `query` support for `:filter-expr`, `:proj-expr` [@ricardojmendez]
* **New**: `update-table` support for index modification [@ricardojmendez]
* **New**: `scan` support for indexes, `:expr-attr-names`, `:proj-expr` [@ricardojmendez]
* **New**: `delete-item` support for expressions [@ricardojmendez]
* **New**: `scan` support for `support filter-expr`, `expr-attr-vals` [@ricardojmendez #90]
* **New**: `scan` support for consistent reads [@ricardojmendez #92]
* **Change**: `update-table` now returns a future instead of a promise (allows exceptions to rethrow)
* **Change**: implementation details now marked as private
* **Fix**: `remove-empty-attr-vals` vs blank strings [@crough #72]

#### MIGRATION INSTRUCTIONS

**[1]**: `(update-item <client-opts> <table> <prim-kvs> <update-map> <opts>)` -> `(update-item <client-opts> <table> <prim-kvs> {:update-map <update-map> <other-opts>})`

**[2]**: `(update-table <client-opts> <table> <throughput> <opts>)` -> `(update-table <client-opts> <table> {:through-put <throughput> <other-opts>})`

## v1.8.0 / 2015 September 26

* **New**: add sanitization multimethod to strip empty values [@jeffh #67]
* **New**: temp hack/workaround to get opt-out `attr-multi-vs` behaviour [#63]
* **New**: support proxy username & password [@tokomakoma123 #68]
* **New**: db-client*: add :keep-alive? option [@kirankulkarni #70]
* **Fix**: don't remove falsey attr vals [#67]
* **Docs**: fix `put-item` docstring [#64, #65]

```clojure
[com.taoensso/faraday "1.8.0"]
```


## v1.7.1 / 2015 June 4

* **Deps**: stop unnecessarily pulling in entire AWS SDK [@jaley #61]
* **Fix**: use DefaultAWSCredentialsProviderChain as provider [@MichaelBlume #59]
* **Performance**: upgrade to Nippy v2.9.0
* **Fix**: relax `AmazonDynamoDBClient` type hint [@ghoseb #62]

```clojure
[com.taoensso/faraday "1.7.1"]
```


## v1.6.0 / 2015 Mar 24

> **BREAKING** release unless upgrading from _v1.6.0-beta1_.

* **BREAK**: `update-item` no longer treats `false` as a special value to denote attribute exists in `:expected`. Attribute existances is now tested for with the `:exists` and `:not-exists` keywords [@mantree].
* **New**: Boolean, Null, Map and List types now supported [@mantree]
* **New**: `update-item` now accepts the comparison operators in `:expected` [@mantree]
* **Fix**: batch operations weren't returning consumed capacity [@johnchapin #49]
* **Fix**: default AWS creds typo preventing federated IAM roles from working, etc. [@shinep #53]
* **Docs**: a number of typo fixes + clarifications [@philippkueng @rmfbarker @sheelc @madeye-matt #50 #52 #55 #58]

```clojure
[com.taoensso/faraday "1.6.0"]
```

## v1.6.0-beta1 / 2015 Jan 12

 * **BREAKING**: `update-item` no longer treats `false` as a special value to denote attribute exists in `:expected`. Attribute existances is now tested for with the `:exists` and `:not-exists` keywords (@mantree).
 * **NEW**: `update-item` now accepts the comparison operators in `:expected` (@mantree).
 * **NEW**: Boolean, Null, Map and List types now supported (@mantree).
 * **FIX** [#49]: Batch operations weren't returning consumed capacity (@johnchapin).


## v1.5.0 / 2014 Jul 26

 * **NEW**: allow reading of binary values written with other (non-serializing) clients.
 * **Fix** [#36]: Batch writes weren't allowing set values.


## v1.4.0 / 2014 May 15

 * **CHANGE** [#29]: `list-tables` now returns a lazy seq and supports >100 tables (@marcuswr, @paraseba).
 * **NEW** [#34]: Add `:query-filter` support to `query` fn (@bpot).


## v1.3.2 / 2014 May 9

 * **FIX** [#32]: deserialization of falsey values (were returning as nil) (@pegotezzi).


## v1.3.0 / 2014 Mar 30

> **NB**: There are **important changes** in this release that should be non-breaking in most cases, but that you should take note of!

### Changes

 * **IMPORTANT** (_usually_ non-breaking): numbers are now returned from DDB as `BigInt`s (previously `Long`s) and `BigDecimal`s (previously `Double`s). This better reflects that way DDB is actually storing numbers internally, and helps preserve number accuracy in some cases.
 * Fix slow lein builds by replacing the joda-time version range used by aws-java-sdk with an explicit dependency on joda-time 2.3.

### New

 * Can now write _unfrozen_ numbers of type: `BigDecimal`, `BigInt`, `BigInteger`. In all cases DDB is limited to 38 bits of precision (use `freeze` when you need more precision).
 * [#28] Add AWSCredentialsProvider support (marcuswr).


## v1.2.0 / 2014 Feb 28

### New

 * `db-client*` can now auto-create a DefaultAWSCredentialsProviderChain instance (joelittlejohn).

### Changes

 * Moved most utils to external `encore` dependency.
 * All fns now take a 'client-opts' arg rather than 'creds' arg. This is non-breaking + purely aesthetic but better represents the arg's purpose.
 * **DEPRECATED**: When providing your own AWSCredentials instance in client-opts please use the `:creds` arg (was `:credentials` before).


## v1.1.1 / 2014 Feb 17

### Fixes

 * #24 Call `distinct` on attr defs (rakeshp).


## v1.1.0 / 2014 Feb 16

### New

 * #23 Add support for Global Secondary Indexes (GSIs) (rakeshp).
 * Bump AWS JDK SDK to 1.7.1.


## v1.0.2 / 2014 Jan 24

### Fixes

 * #15 Fix issue with aws-java-sdk 1.5.x (paraseba).


## v1.0.1 / 2013 Dec 4

### Fixes

 * Add support for arbitrary AWS credentials under `:credentials` key (paraseba).


## v0.12.0 → v1.0.0

  * REVERT AWS Java SDK dependency bump, seems to be causing some issues - will investigate further later.
  * Add docstring examples for `scan`, `query` condition format.
  * Make `scan`, `query` condition format more forgiving: now accepts single vals like `[:eq "Steve]`.
  * Creds can now be an empty map (or nil) to use credentials provider chain (amanas).


## v0.11.0 → v0.12.0

  * Bump AWS Java SDK dependency.


## v0.10.2 → v0.11.0

  * Fix broken `:limit` and segment options.
  * Bump dependencies.


## v0.9.3 → v0.10.2

  * Fix `create-table`, `ensure-table` regression.
  * Auto stringify single-arg keywords to match Carmine v2 API.
    This is _not_ breaking since previous behaviour was just to throw an exception on unfrozen keyword args.


## v0.8.0 → v0.9.3

  * `update-table` now allows throughput specification with just :write or :read units.
  * Added `merge-more` rate-limiting. See batch-op or query/scan docstrings for details.
  * **DEPRECATED**: `block-while-status` -> `table-status-watch`.
  * **BREAKING**: Simplify keydef format: `{:name _ :type _}` -> `[<name> <type>]`.
  * **BREAKING**: Pull mandatory `create-table`, `ensure-table` args out from opts.
  * `update-table` now automatic allows multi-step throughput increases. See docstring for details.

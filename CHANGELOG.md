> This project uses [Break Versioning](https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md) as of **Aug 16, 2014**.

## v1.7.0-RC1 / 2015 Apr 17

> This should be a non-breaking release, but I haven't tested it personally.

* **Change**: stop unnecessarily pulling in entire AWS SDK [@jaley #61]
* **Change**: use DefaultAWSCredentialsProviderChain as provider [@MichaelBlume #59]

```clojure
[com.taoensso/faraday "1.7.0-RC1"]
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

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

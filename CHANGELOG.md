## v1.1.0 / 2014 Feb 16
 * [#23] Add support for Global Secondary Indexes (GSIs) (rakeshp)
 * Bump AWS JDK SDK to 1.7.1.

## v1.0.2 / 2014-01-24
 * [#15] Fix issue with aws-java-sdk 1.5.x (paraseba).

## v1.0.1 / 2013-12-04
 * Hotfix: add support for arbitrary AWS credentials under `:credentials` key (paraseba).


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


## For older versions please see the [commit history][]

[commit history]: https://github.com/ptaoussanis/faraday/commits/master
[API docs]: http://ptaoussanis.github.io/faraday
[Taoensso libs]: https://www.taoensso.com/clojure-libraries
[Nippy GitHub]: https://github.com/ptaoussanis/nippy
[Nippy CHANGELOG]: https://github.com/ptaoussanis/carmine/blob/master/CHANGELOG.md
[Nippy API docs]: http://ptaoussanis.github.io/nippy

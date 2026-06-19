# TODO

## Project tasks
- [x] Overhaul the README and remove out-of-date, unnecessary content (partly done, but still more to do on this one IMO)
- [x] Remove unnecessary scripts like set-sample-env.sh and run-tests
- [x] Remove 'modes' from tests, make everything use DynamoDB local
- [x] Migrate to clojure.test 
- [x] Release 1.10 GA
- [x] Update the Changelog for 1.10 
- [x] Move from Travis CI to GitHub Actions
- [ ] Add a GitHub Action for release
 
## Features
- [x] Transactions
- [ ] Lazy paging

### Table configuration
- [x] TTL
- [ ] Backup
- [ ] Point-in-time recovery
- [ ] Encryption type (+ KMS)
- [ ] Auto-scaling
- [x] On-Demand capacity  
- [ ] Global tables
- [ ] Triggers
- [ ] Tags

## AWS SDK 2 migration

- [x] Client/credentials
- [ ] Non-blocking operations support
- [x] Readme documentation

### Migrate faraday 1 api

- [x] query
- [x] update-ttl
- [x] ensure-table
- [x] batch-write-item
- [x] transact-write-items
- [x] clj-item->db-item
- [x] describe-ttl
- [x] serialize
- [x] get-stream-records
- [x] create-table
- [x] scan-parallel
- [x] batch-get-item
- [x] freeze
- [x] update-table
- [x] get-item
- [x] without-attr-multi-vs
- [x] with-attr-multi-vs
- [x] transact-get-items
- [x] update-item
- [x] put-item
- [x] items-by-attrs
- [x] delete-item
- [x] *attr-multi-vs?*
- [x] scan-lazy-seq
- [x] delete-table
- [x] describe-table
- [x] ensure-ttl
- [x] list-streams
- [x] list-tables
- [x] shard-iterator
- [x] describe-stream
- [x] ex
- [x] db-item->clj-item
- [x] with-thaw-opts
- [x] scan
- [x] remove-empty-attr-vals

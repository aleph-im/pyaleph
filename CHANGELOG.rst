=========
Changelog
=========

Version 0.2.0
=============

- Replaced the P2P service by jsp2pd, an official libp2p daemon. This lifts the dependency on py-libp2p.
- The `--gen-key` option is renamed to `--gen-keys`. It now stores the public key along with the private key,
  and a serialized version of the private key for use by the P2P daemon.
- The private key for the P2P host can no longer be provided through the config.yml file using the `p2p.key`
  field. The key must be provided as a serialized file in the `keys` directory.
- Decommissioned the support for RocksDB. The only supported storage engine is now MongoDB.
- Decommissioned the dockerized VMs as they were replaced by the micro-VMs.
- The message API now supports filtering by content key, start date and end date.
- The "protocol" P2P config is disabled until further notice as it is not working properly.
- Fixed minor issues in the index page of the web service.

Version 0.1
===========

- First version!

=========
Changelog
=========

Version 0.2
===========

- Replaced the P2P service by jsp2pd, an official libp2p daemon. This lifts the dependency on py-libp2p.
- The `--gen-key` option is renamed to `--gen-keys`. It now stores the public key along with the private key,
  and a serialized version of the private key for use by the P2P daemon.
- The private key for the P2P host can no longer be provided through the config.yml file using the `p2p.key`
  field. The key must be provided as a serialized file in the `keys` directory.

Version 0.1
===========

- First version!

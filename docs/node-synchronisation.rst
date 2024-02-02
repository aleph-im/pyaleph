====================
Node Synchronisation
====================

After being deployed, a node needs to synchronise the messages from Aleph.

A few metrics are exposed to monitor this synchronisation, on URL `/metrics`:

1. Total number of messages synchronised: `pyaleph_status_sync_messages_total`
2. Messages downloaded but not processed yet: `pyaleph_status_sync_pending_messages_total`
3. Transactions downloaded but not processed yet: `pyaleph_status_sync_pending_txs_total`

The total number of messages [1] should reach the same value for every node in the Aleph
network. Compare it to other nodes such as https://api2.aleph.im/metrics.json to evaluate
how many messages still need to be synchronised.

The number of pending messages [2] and transactions [3] should reach a value close to zero
when the node is operating, since messages in these queues should be processed by the node.

Ethereum height
---------------

The Aleph node can look for messages on the Ethereum blockchain.
The metric `pyaleph_status_chain_eth_last_committed_height` indicates the number of the
last block synced by the Aleph Node, to be compared with the number of the last block
on the Ethereum chain behind the URL specified in the configuration (mainnet, rinkeby, ...).

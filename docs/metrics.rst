=======
Metrics
=======

Introduction
------------

To facilitate your monitoring effort, pyaleph expose some metrics on the server health and synchronisation status.

Theses metrics are available in  two formats: JSON  at `/metrics.json` on the web port or in the
`Prometheus format <https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md>`_ at the standard `/metrics`.

-----------
Description
-----------

Below are the current metrics, a lot more are coming in the future as we improve this interface.


+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| Field                                           | Type          | Description                                                                                |
+=================================================+===============+============================================================================================+
| pyaleph_build_info                              | dict          | Number of transactions that are pending treatment or need to be retried                    |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_peers_total                      | int           | Number of transactions that are pending treatment or need to be retried                    |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_messages_total              | int           | Total number of treated message                                                            |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_pending_messages_total      | int           | Number of messages that are pending treatement or need to be retried                       |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_messages_total              | int           | Number of messages that are pending treatement or need to be retried                       |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_pending_txs_total           | int           | Number of transactions that are pending treatment or need to be retried                    |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_messages_reference_total    | Optional[int] | Last know commit from reference point (see configuration aleph.setting reference_node_url) |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_messages_remaining_total    | Optional[int] | Difference with the refrence point                                                         |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_chain_eth_last_committed_height  | Optional[int] | Last treated commit in the ETH chain                                                       |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_chain_eth_height_reference_total | Optional[int] | Last known commit in the ETH Chain                                                         |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_chain_eth_height_remaining_total | Optional[int] | Difference between last handled and max ETH commit                                         |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_processing_pending_messages_*           | Optional[int] | Internal, for optimisation effort                                                          |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+

Use with prometheus
-------------------

To use with prometheus simply add <your server url>:4024/metrics as a target in your prometheus.py. Eg prometheus file.

.. code-block:: yaml

   global:  scrape_interval:     5s
   evaluation_interval: 5s

   scrape_configs:
   - job_name: aleph_demo
     static_configs:
       - targets: ['pyaleph:4024']


Make sure your http port is accessible from the prometheus server.

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
| pyaleph_status_peers_total                      | int           | Total number of peers                                                                      |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_messages_total              | int           | Total number of treated message                                                            |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_permanent_files_total       | int           | Total number of permanent files stored                                                     |
+-------------------------------------------------+---------------+--------------------------------------------------------------------------------------------+
| pyaleph_status_sync_pending_messages_total      | int           | Number of messages that are pending treatement or need to be retried                       |
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


HTTP API metrics and request log
--------------------------------

The HTTP API records per-route metrics, exposed on `/metrics` next to the node metrics above, and writes one JSON
log line per request.

-------
Metrics
-------

The ``route`` label is the matched route template (for example ``/api/v0/messages/{item_hash}``), never the raw path,
or ``unmatched`` when no route matched (404, 405). The ``method`` label is one of ``GET``, ``HEAD``, ``POST``, ``PUT``,
``PATCH``, ``DELETE`` and ``OPTIONS``, or ``OTHER`` for any other method (the request log keeps the real method).

A handler timeout is counted with status ``504``. A handler that is cancelled, for example when the server shuts down,
is counted with status ``499``. Client disconnects are not detected: the gunicorn aiohttp worker does not cancel a
handler when its client goes away, so such a request is counted with the status the handler eventually returns.

+------------------------------------------+-----------+---------------------------+--------------------------------------------+
| Metric                                   | Type      | Labels                    | Description                                |
+==========================================+===========+===========================+============================================+
| aleph_http_requests_total                | counter   | route, method, status     | Number of HTTP requests handled            |
+------------------------------------------+-----------+---------------------------+--------------------------------------------+
| aleph_http_request_duration_seconds      | histogram | route, method             | Request handling time, in seconds          |
+------------------------------------------+-----------+---------------------------+--------------------------------------------+
| aleph_http_requests_in_flight            | gauge     | route                     | Requests currently being handled           |
+------------------------------------------+-----------+---------------------------+--------------------------------------------+
| aleph_http_response_bytes_total          | counter   | route                     | Response body bytes (Content-Length)       |
+------------------------------------------+-----------+---------------------------+--------------------------------------------+
| aleph_http_request_bytes_total           | counter   | route                     | Request body bytes received                |
+------------------------------------------+-----------+---------------------------+--------------------------------------------+

The request bytes are the body bytes the server actually received, whatever Content-Length the client declared.

The response bytes are the declared Content-Length of each response, so an aborted download counts in full. Responses
without a Content-Length (most streams, WebSockets) add 0.

-----------
Request log
-----------

Each request produces one JSON line on stdout, on the ``aleph.http`` logger, with these keys:

``ts``, ``req_id``, ``method``, ``route``, ``path``, ``query``, ``query_keys``, ``status``, ``dur_ms``, ``bytes_in``,
``bytes_out``, ``client``, ``ua``, ``sender``, ``auth``.

* ``path`` is truncated to 2048 characters, ``ua`` (the User-Agent) to 256.
* ``query`` is the raw query string, truncated to 512 characters. ``query_keys`` lists the distinct parameter names.
* ``bytes_in`` and ``bytes_out`` follow the request and response bytes metrics above.
* ``client`` is the first address of the ``X-Forwarded-For`` header, falling back to the peer address, truncated to 256
  characters. The header is trusted without verification, because the node is expected to sit behind its own proxy: do
  not expose the API port directly, or clients can spoof this field.
* ``sender`` is the sender address of a message submitted through ``/api/v0/messages``, the p2p pub endpoint or an
  upload endpoint that carries a message (``/api/v0/ipfs/add_file``, ``/api/v0/ipfs/add_car``,
  ``/api/v0/storage/add_file``), else ``null``. Uploads are attributed once their message is broadcast.
* ``auth`` is ``claimed`` when a sender is set and ``anonymous`` otherwise. It is not ``signed`` on purpose: the request
  path only validates the structure of the message, signatures are verified later in the processing pipeline. A
  ``claimed`` sender is therefore what the client declared, not an authenticated identity.

gunicorn's own access log is off by default; set ``CCN_CONFIG_API_ACCESS_LOG=1`` to enable it.

----------
Request id
----------

Every response carries an ``X-Request-Id`` header, and the same value is logged as ``req_id``. A client-supplied
``X-Request-Id`` is reused if it matches ``^[A-Za-z0-9._-]{1,64}$``, otherwise a new id is generated.

----------------
Multiple workers
----------------

The API runs several gunicorn workers. HTTP metrics are aggregated across workers with prometheus_client's
multiprocess mode: ``run_aleph_ccn_api.sh`` exports ``PROMETHEUS_MULTIPROC_DIR`` (a fresh directory under ``/tmp`` unless
already set) before gunicorn starts, and ``deployment/scripts/gunicorn_conf.py`` removes the gauge files of dead workers
through a ``child_exit`` hook, so ``aleph_http_requests_in_flight`` does not drift after a worker is killed.

If you set ``PROMETHEUS_MULTIPROC_DIR`` yourself, the directory must be empty when the API starts: files left by a
previous run would be read and added to the metrics.

When the API runs in a single process (no ``PROMETHEUS_MULTIPROC_DIR``), ``/metrics`` also carries prometheus_client's
``process_*`` and ``python_*`` collectors. They are absent in multiprocess mode, so dashboards must not rely on them.

--------------------
Example Loki queries
--------------------

5xx responses:

.. code-block:: text

   {container="pyaleph-api"} | json | __error__="" | status >= 500

Requests per route over the last hour:

.. code-block:: text

   sum by (route) (count_over_time({container="pyaleph-api"} | json route | __error__="" [1h]))

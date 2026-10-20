=================
Credit ledger v2
=================

The credit ledger is computed inside the CCN from chain top-ups, metering
attestations and aleph messages, as hourly periods folded in order. Every CCN
reaches the same ledger; the legacy distribution posts are no longer applied.

Configuration (``aleph.credits``)
=================================

``serve_v2``
    Switch the node to the v2 ledger. Leave ``false`` during the shadow run.
    It has two effects, gated differently (see "What ``serve_v2`` changes"
    below): ingest decisions on ledger inputs follow the flag immediately,
    reads switch only once the node has closed its first period.
``genesis.hash``
    Item hash (STORE, storage engine) of the genesis file.
``genesis.period``
    H0, the genesis period, unix seconds on an hour boundary.
``chain.rpc_url``
    Ethereum RPC, falls back to ``ethereum.api_url``. It must serve historical
    state to quote old ALEPH or ETH top-ups; Infura is sufficient. USDC top-ups
    need none.
``chain.start_block``
    First block with timestamp >= H0. Required: unset, top-ups are not
    indexed, the chain cursor never advances and no period ever closes.
``chain.contract``, ``chain.usdc``, ``chain.aleph``, ``chain.chainlink_eth_usd``
    Contract and token addresses.
``chain.aleph_weth_pool``
    Uniswap v3 ALEPH/WETH pool for the ALEPH TWAP. Must be set: unset, ALEPH
    top-ups are stored ``unquoted`` (never credited) until it is.
``chain.denylist``
    Senders whose transfers to the contract are not top-ups (lowercase
    compare). Also the default denylist of the genesis export.
``chain.finality_blocks``, ``chain.chunk_blocks``
    Indexer finality depth and scan chunk size.
``topup_bonus.aleph``
    Bonus on ALEPH top-ups.
``attestation.addresses``, ``attestation.channels``, ``attestation.post_type``
    Who may attest metering, and where.
``attestation.start_period``
    First folded period (H0).
``attestation.grace_seconds``
    How long to wait for an attestation before closing a period without it.
``fold.checkpoint_every_hours``
    Checkpoint cadence.

Genesis export
==============

On the reference CCN (api2), once the legacy service is stopped::

    python scripts/credits_export_genesis.py --config config.yml \
        --period <H0 unix> [--denylist <addr,addr,...>] --out genesis.json

The denylist is ``credits.chain.denylist`` from the config (the two
2026-10-19 distributions to the Uniswap v4 PoolManager, among others), plus
any address given with ``--denylist``.

The lots are replayed from ``credit_history`` with the legacy FIFO (the code
of ``aleph.repair``), over the history observed before H0, so they equal the
legacy balance at that instant. Lots expired at H0 (expirations compared in
whole seconds), lots of denylisted addresses and history rows observed at or
after H0 (grants and debits, listed as ``late_post``) are dropped. Addresses
are normalized and lots of the same address and expiration are merged. The
script prints per-address totals (compare them with api2
``/credit_history/summary``), the excluded rows, the sha256 of the file and
the config lines to paste (``credits.genesis.hash``, ``credits.genesis.period``,
``credits.attestation.start_period``). It exits 1 on a database error.

The file is a JSON list ``[{address, amount, expiration (unix s or null),
ref}]``, compact and key-sorted. Upload it as a STORE (storage engine): its
item hash is that sha256, to be set as ``credits.genesis.hash``.

Things to know about the export:

* It is reproducible only on the reference CCN. The observed time of a
  history row is the referenced message's ``first_confirmed_at``, falling
  back to the node-local ``reception_time``, so another CCN can order or cut
  rows differently around H0. The genesis is authoritative by its pinned
  hash, not by being re-exportable elsewhere.
* It replays ``credit_history``, not the ``credit_balances`` cache. An account
  repaired only in the cache is exported from its unrepaired history; an
  account drained to zero by a compromised expense post is exported with no
  row unless its history is repaired first.
* H0 must be at or after the legacy stop (step 3): a post the legacy service
  makes at or after H0 is a ``late_post``, never part of the genesis.

What ``serve_v2`` changes
=========================

* **Ingest decisions on ledger inputs follow the flag immediately.** With
  ``serve_v2 = true`` a well-formed credit transfer is always PROCESSED (the
  fold settles it at its confirmation hour, or excludes it as
  ``insufficient_balance``), and ``aleph_credit_distribution`` and
  ``aleph_credit_expense`` posts are schema-checked but write nothing. This
  does not wait for the fold: a verdict that depended on node-local state
  would let a transfer be REJECTED on one node and PROCESSED on another,
  forever. The flag must therefore change on every CCN in the same release.
* **Reads switch at the node's first closed period.** Balance API, ingest
  admission check, removal cron and credit history serve the v2 ledger only
  once the fold has closed a period. Until then the node logs an ERROR and
  keeps serving the legacy tables (``credit_balances`` as last written, no
  longer fed by distribution or expense posts nor by transfers), so balances
  are frozen at their last legacy value for that window.

Cutover checklist
=================

Follows spec section 11.

1. **Shadow run.** Release with the v2 tables and the fold configured
   (``credits.attestation.start_period``, ``credits.chain.start_block``) and
   ``serve_v2 = false`` on every CCN. Legacy behaviour is unchanged. Compare
   ``/credits/periods/{period}`` hashes across CCNs (below) and the per-node
   totals with the last expense posts.
2. **Pick H0** (an hour start) and announce the billing changes.
3. **Stop the legacy credit service at least one hour before H0**, after its
   last expense post for the hour before H0, once the distribution job has
   drained the indexer. H0 is at or after the stop: nothing the legacy
   service writes may fall after H0. Reconcile by hand any contract transfer
   between the last distribution post and H0 (contract event log).
4. **Repair, then export.** On the reference CCN run ``pyaleph --repair``
   (rebuilds the ``credit_balances`` cache from ``credit_history``) and
   reconcile ``credit_balances`` with the replay: any difference is a history
   problem the export would carry (see above). Then run the export and review
   its totals and excluded rows against api2.
5. **Publish** ``genesis.json`` as a STORE.
6. **Enable release, on every CCN at once**, with:

   * ``credits.genesis.hash`` and ``credits.genesis.period`` (H0);
   * ``credits.attestation.start_period`` (H0);
   * ``credits.chain.start_block`` (first block with timestamp >= H0;
     required, or no period ever closes);
   * ``credits.chain.aleph_weth_pool`` (or ALEPH top-ups stay unquoted);
   * ``credit_balances.addresses`` emptied (the compromised credit address
     must not be whitelisted anywhere);
   * ``serve_v2 = true``.

   Every CCN loads the genesis (idempotent by hash and index; a different
   hash is refused once one is loaded) and folds from H0. Ingest decisions
   switch at once; reads switch at each node's first closed period.
7. **Nodestatus** switches to ``/credits/revenue`` at the first distribution
   after H0, with explicit heights. The revenue range stops at the first
   period without a stored ALEPH quote; the fold job re-quotes such periods
   (the oldest 168 per run, over the whole closed range), so a retry later
   completes it.

Refold
======

Start the node with ``--credits-refold-from <unix hour>`` to mark the ledger
dirty from that period; the fold job rebuilds it, genesis rows included.

Comparing CCNs
==============

``GET /api/v0/credits/periods/{period}`` returns ``period_hash`` and, on
checkpoint periods, ``checkpoint_hash``. Two CCNs agree when the hashes of the
same period are equal; on a difference, compare earlier periods to find the
first divergence.

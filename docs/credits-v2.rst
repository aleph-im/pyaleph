=================
Credit ledger v2
=================

The credit ledger is computed inside the CCN from chain top-ups, metering
attestations and aleph messages, as hourly periods folded in order. Every CCN
reaches the same ledger; the legacy distribution posts are no longer applied.

Configuration (``aleph.credits``)
=================================

``serve_v2``
    Serve the credit APIs from the ledger instead of the legacy tables. Leave
    ``false`` during the shadow run.
``genesis.hash``
    Item hash (STORE, storage engine) of the genesis file.
``genesis.period``
    H0, the genesis period, unix seconds on an hour boundary.
``chain.rpc_url``
    Ethereum RPC, falls back to ``ethereum.api_url``. It must serve historical
    state to quote old ALEPH or ETH top-ups; Infura is sufficient. USDC top-ups
    need none.
``chain.start_block``
    First block with timestamp >= H0. Unset: top-ups are not indexed.
``chain.contract``, ``chain.usdc``, ``chain.aleph``, ``chain.chainlink_eth_usd``
    Contract and token addresses.
``chain.aleph_weth_pool``
    Uniswap v3 ALEPH/WETH pool for the ALEPH TWAP. Unset: ALEPH top-ups are
    stored ``unquoted`` until it is set.
``chain.denylist``
    Senders whose transfers to the contract are not top-ups (lowercase compare).
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
        --period <H0 unix> --denylist <addr,addr,...> --out genesis.json

The lots are replayed from ``credit_history`` with the legacy FIFO (the code
of ``aleph.repair``), over the history observed before H0, so they equal the
legacy balance at that instant. Lots expired at H0, lots of denylisted
addresses (the two 2026-10-19 distributions to the Uniswap v4 PoolManager) and
grants observed at or after H0 are dropped. Addresses are normalized and lots
of the same address and expiration are merged. The script prints per-address
totals (compare them with api2 ``/credit_history/summary``), the excluded rows,
and the sha256 of the file.

The file is a JSON list ``[{address, amount, expiration (unix s or null),
ref}]``, compact and key-sorted. Upload it as a STORE (storage engine): its
item hash is that sha256, to be set as ``credits.genesis.hash``.

Cutover checklist
=================

1. Stop the legacy credit service and note the last distribution post.
2. Reconcile by hand any transfer between that post and H0 (check the contract
   event log for that window).
3. Run the export; review the totals and excluded rows against api2.
4. Upload ``genesis.json`` as a STORE. Set ``credits.genesis.hash``,
   ``credits.genesis.period``, ``credits.chain.start_block`` and
   ``credits.attestation.start_period`` in the default config.
5. Shadow run with ``serve_v2 = false`` and compare the hashes (below).
6. Release. Every CCN loads the genesis (idempotent by hash and index) and
   folds from H0. A different hash is refused once one is loaded.
7. Set ``serve_v2 = true`` once the hashes agree.

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

# FORGET permission delegation

## Problem

The FORGET message handler enforces a strict sender-equality rule that
bypasses the rest of the permission system:

```python
# src/aleph/handlers/content/forget.py:140
if target_message.sender != message.sender:
    raise PermissionDenied(
        f"Cannot forget message {target_hash} because it belongs to another user"
    )
```

This rule is independent of the `security`-aggregate delegation that
governs every other message type. The practical consequences:

1. An owner O who delegated (say) STORE permission to D1 has no way
   to forget the resulting content even after revoking D1, because
   `target.sender = D1` and the FORGET's sender is O.
2. A second delegate D2 (granted by O for cleanup purposes) cannot
   forget D1's content either, for the same reason.
3. Conversely, D1 retains the ability to forget any message they
   signed even after O revokes their delegation, because the check
   never consults the security aggregate.

Result: an owner cannot remediate unwanted content uploaded by a
former delegate. The only path is to ask the now-distrusted delegate
to clean up after themselves.

## Goal

Replace the strict sender-equality rule with a per-target
authorization check that uses the existing `security`-aggregate
delegation system. Authorization to forget a message is determined
by the target's `content.address`, not by who signed it.

This brings FORGET into line with how every other message type
treats ownership (aggregates, posts, balances all keyed by
`content.address`).

## Non-goals

- No changes to the `security` aggregate schema. The existing
  `types: [...]` filter already supports `"FORGET"` as a value.
- No DB schema changes or migrations.
- No new HTTP endpoints. Message submission is unchanged.
- No new "admin" or super-user concept.
- No changes to how FORGET-of-FORGET, VM-volume blocking, or
  already-forgotten / rejected / removed targets are handled.

## Authorization rule

A FORGET message with sender `S` succeeds iff:

1. **Self-authorization (existing base check, unchanged).** `S` is
   authorized to act for the FORGET message's own `content.address`
   via `check_sender_authorization`. Because this check honors the
   `types: [...]` filter on the security aggregate, a delegate whose
   grant excludes `"FORGET"` is blocked here.

2. **Per-target authorization (new).** For each target message `T`
   referenced by the FORGET (after expanding `content.aggregates`
   to their element hashes), `S` must be authorized to act for
   `T.parsed_content.address` with respect to the FORGET message.
   The same security-aggregate logic is reused; the only new thing
   is that the owner address is supplied explicitly rather than
   read off the FORGET's own content.

3. **All-or-nothing.** Every target is validated in
   `check_permissions` before any forget is applied. The first
   failed target raises `PermissionDenied` and `process()` never
   runs. This guarantee is structural: the message pipeline calls
   `check_permissions` to completion before invoking `process`.

### Behavior changes

- *Closed:* O (or a delegate of O with FORGET in `types`) can now
  forget any message whose `content.address = O`, regardless of
  who signed it.
- *Tightened:* A former delegate D1 can no longer forget messages
  they signed with `content.address = O` once O has revoked their
  delegation. The right to manage that content belongs to its
  declared owner, not its signer.
- *Unchanged:* A sender forgetting messages they signed with
  `content.address = themselves` still works (it is just the
  general rule applied to the self-owner case).

### Retained edge cases

- Targets in `FORGOTTEN`, `REJECTED`, or `REMOVED` status are
  skipped without an authorization check (there is nothing to
  authorize).
- FORGET targeting another FORGET still raises
  `CannotForgetForgetMessage`.
- Files referenced by VM volumes still raise `ForgetNotAllowed`.

## Code changes

Two files. No schema, no migration, no API surface change.

### `src/aleph/permissions.py`

Promote the existing private `_check_delegated_authorization`
helper to a public function with a clearer name. The body is
unchanged; only the name and visibility change.

```python
def is_sender_authorized_for_owner(
    session: DbSession, sender: str, owner_address: str, message: MessageDb
) -> bool:
    # same security-aggregate lookup as before; same types /
    # channels / chain / post_types / aggregate_keys filtering.
    ...
```

`check_sender_authorization` continues to call this internally.
Its existing behavior is preserved.

### `src/aleph/handlers/content/forget.py`

In `ForgetMessageHandler.check_permissions`, replace the strict
sender-equality block with the per-target authorization call:

```python
# remove:
if target_message.sender != message.sender:
    raise PermissionDenied(
        f"Cannot forget message {target_hash} because it belongs to another user"
    )

# replace with:
target_owner = target_message.parsed_content.address
if not is_sender_authorized_for_owner(
    session=session,
    sender=message.sender,
    owner_address=target_owner,
    message=message,
):
    raise PermissionDenied(
        f"Sender {message.sender} is not authorized to forget message "
        f"{target_hash} owned by {target_owner}"
    )
```

The rest of the loop (status filtering, FORGET-of-FORGET check,
target-found check) is kept verbatim.

### Type safety

`target_message.parsed_content` is one of `StoreContent`,
`PostContent`, `AggregateContent`, `InstanceContent`, or
`ProgramContent`; all expose `.address`. FORGET content also has
`.address` but FORGET-targeting-FORGET is rejected earlier in the
same loop, so we never read `.address` off a FORGET. No `hasattr`
guard is needed.

## Test plan

Tests live in `tests/message_processing/test_process_forgets.py`,
extending the existing fixtures.

### Positive cases (FORGET succeeds)

1. **Self-ownership regression.** O signs a STORE with
   `content.address = O`, then O sends a FORGET. Confirms today's
   happy path.
2. **Owner forgets delegate-created content.** O delegates STORE
   to D1; D1 signs a STORE with `content.address = O`; O sends a
   FORGET targeting it. Succeeds.
3. **Second-delegate cleanup.** O grants `types: ["FORGET"]` to
   D2; D2 forgets a STORE whose `content.address = O`.
4. **Full original scenario.** Combines #2 and #3: D2 cleans up
   content originally created by D1 on behalf of O.
5. **Forget survives revocation of an unrelated permission.** O
   revokes D1's STORE delegation, then O sends a FORGET against
   content created earlier under that delegation. Succeeds.
6. **Aggregate field expansion.** O has an aggregate; O sends a
   FORGET with `aggregates: [key]`. Per-target check passes for
   each expanded element.
7. **Cross-owner multi-target.** S is delegated FORGET by both
   O1 and O2. A single FORGET listing targets owned by both
   succeeds.

### Negative cases (FORGET denied)

8. **Stranger.** X with no delegation tries to forget O's STORE.
   Denied.
9. **Revoked delegate (intended tightening).** D1 had STORE
   delegation, created content with `content.address = O`, was
   revoked. D1 tries to forget. Denied. Documents the behavior
   change relative to the old strict-equality rule.
10. **Delegate scoped away from FORGET.** O grants
    `types: ["STORE"]` to D1; D1 tries to forget. Denied because
    the `types` filter excludes FORGET.
11. **No claim over self-signed delegate content.** D1 signs a
    STORE with `content.address = D1` while delegated by O; O
    tries to forget that STORE. Denied, because O has no
    authorization claim over `content.address = D1`.
12. **All-or-nothing on mixed targets.** A FORGET lists three
    hashes; the second one fails authorization. Assert
    `PermissionDenied` is raised and that none of the three
    targets are in `FORGOTTEN` state afterwards.

### Existing tests to revisit

Any test in `test_process_forgets.py` that asserts "delegate
cannot forget owner's content" under the strict-equality rule
needs to be re-read and revised. Implementation pass will
inspect the file and call out which existing cases flip from
negative to positive (or get removed because the underlying gap
is now closed).

### Out of scope for tests

- No schema or migration tests (no schema change).
- No new API tests (FORGET goes through the same message
  submission path that is already covered).

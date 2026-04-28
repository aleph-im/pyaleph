# How Smart Wallet Signature Verification Works

> Companion to [`smart-wallet-signatures.md`](./smart-wallet-signatures.md), which
> documents the signature *format*. This document explains the verification *flow*
> and the **security guarantees** it provides for each signer type.

---

## 1. Three signer types, three verification paths

Pyaleph's `EVMVerifier` handles three very different kinds of signers behind the same
`verify_signature` entry point:

| Signer type | Signature shape | RPC calls | Authority |
|---|---|---|---|
| **EOA** (MetaMask, Ledger, …) | 65-byte ECDSA | 0 | Private key (eternal) |
| **Deployed smart wallet** (ERC-1271) | Any bytes the contract accepts | 2 (`get_code` + `isValidSignature`) | Current contract state |
| **Counterfactual smart wallet** (ERC-6492) | `abi.encode(factory, calldata, innerSig) + 0x6492…6492` | 1 (`UniversalSigValidator`) | Factory calldata (frozen at CREATE2 time) |

Detection is upfront and cheap: if the signature ends with the ERC-6492 magic suffix,
route to the counterfactual path. Otherwise try plain ECDSA; if that fails and the
sender has deployed bytecode, route to ERC-1271.

---

## 2. Counterfactual verification (before deployment)

### Mechanical flow in pyaleph

```
1. Detect 0x6492…6492 suffix  → present
2. Build deploy_data = ValidateSigOffchainBytecode + abi.encode(
       sender, messageHash, fullSignature
   )
3. eth_call({"data": deploy_data})  # no `to` field = contract creation
4. Check the single returned byte == 0x01
```

ERC-6492 uses the **contract-creation pattern** — there is no pre-deployed
validator contract on chain. The bytecode is sent as creation data; it runs as
a constructor and performs two operations atomically:

1. **Simulates deployment**: `factory.call(factoryCalldata)` — this deploys the
   wallet contract at some address, purely inside the simulation. No state changes
   persist because we use `eth_call`, not `sendTransaction`.
2. **Calls `isValidSignature`**: routes the inner signature through the deployed
   contract's verification logic, and returns a single byte (`0x01` valid /
   `0x00` invalid) from the constructor's RETURN opcode.

The canonical bytecode is the EIP-6492 reference implementation from
AmbireTech/signature-validator, shipped at
`src/aleph/chains/assets/erc6492_validator_bytecode.hex`.

### What the simulation does (Kernel example)

After the factory call, a Kernel wallet exists (inside the simulation) at some
CREATE2 address. Its `isValidSignature`:

1. Parses the 86-byte inner sig → `(type=0x01, validator=0x845A…4cE57, ecdsa)`
2. Dispatches to the ECDSA validator plugin configured as **root validator**
3. The plugin wraps the Aleph hash in its own EIP-712 domain (with `block.chainid`
   and wallet address)
4. Runs `ecrecover` on the wrapped hash with the 65-byte ECDSA
5. Compares the recovered address to the **owner baked into `factoryCalldata`**
6. Returns the ERC-1271 magic value `0x1626ba7e` on match

### Security guarantees

1. **The Aleph hash binds to the message.**
   The hash is `keccak256(EIP-191(chain + sender + type + item_hash))`. Any
   tampering with sender, item content, or chain invalidates the signature.

2. **The sender address is cryptographically bound to the wallet's config.**
   `sender = keccak256(0xff || factory || salt || keccak256(initcode))[12:]`.
   The initcode embeds the owner (via `factoryCalldata`). Swapping the owner
   changes the initcode → changes the CREATE2 address → no longer equals
   `sender`. Finding a colliding `(factory, salt, initcode)` tuple is a 160-bit
   preimage search — computationally infeasible.

3. **Only the owner's private key could produce the inner ECDSA.**
   The ECDSA validator recovers a key and compares it against the configured
   owner. Without that private key, forging the inner sig would require
   breaking secp256k1.

### Attacks defeated

| Attack | Why it fails |
|---|---|
| Submit ERC-6492 with a malicious factory | Simulated deployment lands at a different address → subsequent `isValidSignature` call hits empty bytecode → invalid |
| Swap the owner inside `factoryCalldata` | Changes the initcode → changes the CREATE2 address → no longer matches `sender` |
| Replay signature on a different Aleph message | Hash depends on `item_hash`, `sender`, `chain`, `type`; any change invalidates |
| Replay across chains | Kernel's EIP-712 domain includes `block.chainid` (also why pyaleph is scoped to ETH mainnet only until per-chain RPCs are wired) |

### Residual trust assumptions

- Anyone who obtains the owner's private key can sign as `sender` — same risk as
  any EOA.
- Trust in the `ValidateSigOffchain` / `UniversalSigValidator` bytecode. The
  reference implementation is documented in EIP-6492 and sourced from
  AmbireTech/signature-validator. Worth verifying the packaged hex against the
  upstream source before relying on it in production.

---

## 3. Deployed-wallet verification (ERC-1271, the normal case)

Once the smart wallet has been deployed (first on-chain transaction), clients stop
sending the ERC-6492 wrapper. They send only the inner signature (86 bytes for
Kernel) directly.

### Mechanical flow in pyaleph

```
1. Detect 0x6492…6492 suffix → NOT present
2. Try plain ECDSA ecrecover → fails (sender is a contract, not an EOA)
3. eth_call → w3.eth.get_code(sender) → non-empty = deployed
4. eth_call → sender.isValidSignature(hash, innerSig)
5. Check return == 0x1626ba7e
```

No factory, no simulation, no counterfactual reasoning. We query the actual
on-chain contract at `sender`, and the contract itself is the authority on whether
the signature is valid.

### What the contract does internally (Kernel example)

Same as step 2→6 of the counterfactual flow above, but reading state from the
actual deployed contract instead of a simulated one:

1. Parses 86-byte sig → `(type, validator, ecdsa)`
2. Routes to the **currently configured** root validator plugin
3. The plugin wraps the Aleph hash in EIP-712
4. `ecrecover` on the wrapped hash
5. Compares recovered address to the **currently configured owner**
6. Returns `0x1626ba7e` if match

### Crucial difference vs EOA / counterfactual

Smart wallets have **mutable authorization**:

- **Key rotation:** if the wallet rotates from Key-A to Key-B after signing, most
  validators (including Kernel's ECDSA validator) will reject the old signature
  on re-verification. Signatures effectively expire.
- **Multi-validator setups:** a wallet can later add session keys, passkeys, or
  multisig plugins. Any of those authorized signers can also produce valid sigs
  as `sender` — not just the original EOA.
- **Upgradeable logic:** if the contract is upgradeable, a malicious upgrade
  could flip `isValidSignature` to always return true. Trust boundary = whoever
  controls the upgrade keys.

Contrast with EOAs: an ECDSA signature produced once is valid forever against the
same address, because the "address" is literally `keccak256(pubkey)[12:]` — there
is no state to mutate.

### Security guarantees

- ✅ The contract at `sender` **right now** approves this signature for this hash.
- ✅ Whoever holds currently-authorized keys signed this message.
- ✅ No address collision: the contract is queried directly, not reconstructed.
- ⚠️ "Owner at signing time" may differ from "owner at verification time".

### Attacks defeated

Same list as counterfactual, minus the factory-related ones (no factory involved).
Replay across chains is mitigated by ETH-mainnet-only scoping.

---

## 4. Applied to the real example

For Aleph message
[`6f699b25…1a26d`](https://api2.aleph.im/api/v0/messages/6f699b252db10e65e8651a77289a7789e2da77ce26c4f4ad247fbe9bd1e1a26d):

```
Aleph sender:   0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635  (Kernel smart wallet, counterfactual)
Owner EOA:      0xfFFEfCDE25e1d00474530f1A7b90D02CEda94fD7  (private key holder)
Factory:        0xd703aae79538628d27099b8c4f621be4ccd142d5
Validator:      0x845ADb2C711129d4f3966735eD98a9F09fC4cE57
```

The message arrives with the ERC-6492 wrapper because the wallet isn't deployed
yet. Pyaleph:

1. Detects the `0x6492…6492` suffix → routes to `UniversalSigValidator`.
2. `eth_call` to `0x0000…3823` simulates deploying a Kernel at `0xa9F3…1635`
   configured with owner `0xfFFE…4fD7`.
3. Calls `isValidSignature` on that simulated contract with the 86-byte inner
   sig.
4. The ECDSA validator recovers an address from the EIP-712-wrapped hash and
   compares it to `0xfFFE…4fD7`.
5. If it matches, returns `0x1626ba7e` → pyaleph accepts the message as signed by
   `0xa9F3…1635`.

At some later point, the user performs their first on-chain transaction and the
wallet is deployed. From that message onward, Privy sends only the 86-byte inner
sig (no wrapper). Pyaleph's `EVMVerifier` then routes through the ERC-1271 path
instead — same guarantees, fewer bytes, one fewer simulated step.

---

## 5. What the verification does NOT guarantee

Worth spelling out, so we don't over-promise:

- **Liveness of the owner's key.** If the key is lost or stolen, the attacker can
  sign as `sender`. This is identical to any EOA risk.
- **Correctness of contract logic.** We trust that the Kernel contract and its
  plugins implement `isValidSignature` faithfully. A buggy or malicious wallet
  implementation could accept arbitrary signatures.
- **Immutability over time for deployed wallets.** A signature that verified
  today may not verify tomorrow if the wallet's config changes. Pyaleph's model
  is "valid at time of verification", not "valid forever".
- **Cross-chain uniqueness.** A signature produced for a wallet on Base is not
  validatable on Ethereum mainnet because the EIP-712 domain uses `block.chainid`.
  This is why pyaleph currently only enables smart-wallet verification on
  `Chain.ETH`.

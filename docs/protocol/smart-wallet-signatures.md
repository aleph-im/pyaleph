# ERC-6492 / ERC-1271 Smart Wallet Signatures in Aleph

> Technical reference for smart contract wallet signature verification, generated from
> analysis of the real Aleph message
> `6f699b252db10e65e8651a77289a7789e2da77ce26c4f4ad247fbe9bd1e1a26d`.

---

## 1. Context: why these signatures appear

Providers such as **Privy** create **smart account** wallets (ZeroDev Kernel) for
users. These contracts can exist **counterfactually** — the address is deterministic
(CREATE2), but the bytecode is not on chain until someone pays the gas for the first
deployment.

When a user signs an Aleph message with Privy before their wallet has been deployed:

- Privy produces an **ERC-6492** signature that bundles the deployment instructions
  for the wallet together with the inner ECDSA signature.
- Before this change, pyaleph did not understand this format and rejected the signature
  as invalid.

---

## 2. Contract stack involved (real-world example)

| Address | Role |
|---|---|
| `0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635` | **Kernel smart wallet** (Aleph sender). Not yet deployed. |
| `0xd703aae79538628d27099b8c4f621be4ccd142d5` | **Factory** — deploys the smart wallet via `createAccount()` |
| `0xaac5d4240af87249b3f71bc8e4a2cae074a3e419` | **Kernel implementation** — the logic singleton |
| `0x845ADb2C711129d4f3966735eD98a9F09fC4cE57` | **ECDSA Validator plugin** — verifies that the owner signed |
| `0xfFFEfCDE25e1d00474530f1A7b90D02CEda94fD7` | **Owner EOA** — the private key that actually produces the ECDSA signature |

---

## 3. Full schema of an ERC-6492 signature

```
FULL SIGNATURE (hex)
│
├── ABI-encoded(address factory, bytes calldata, bytes innerSig)
│   │
│   ├── [0:32]   factory            = 0xd703aae79538628d27099b8c4f621be4ccd142d5 (padded)
│   ├── [32:64]  offset of calldata = 0x60 (96)
│   ├── [64:96]  offset of innerSig = 0x260 (608)
│   │
│   ├── CALLDATA (452 bytes) — instructions for the factory
│   │   ├── selector: 0xc5265d5d  → createAccount(address impl, bytes initData, uint256 index)
│   │   ├── impl:     0xaac5d4240af87249b3f71bc8e4a2cae074a3e419
│   │   ├── index:    0
│   │   └── initData (Kernel initialize, 292 bytes):
│   │       ├── selector:        0x3c3b752b  → initialize(...)
│   │       ├── validator type:  0x01  (root ECDSA validator)
│   │       ├── validator plugin: 0x845ADb2C711129d4f3966735eD98a9F09fC4cE57
│   │       └── owner EOA:       0xfFFEfCDE25e1d00474530f1A7b90D02CEda94fD7  ← real private key
│   │
│   └── INNER SIGNATURE (86 bytes) — signature produced by the owner EOA
│       ├── [0]     type byte:  0x01  (root validator)
│       ├── [1:21]  validator:  0x845ADb2C711129d4f3966735eD98a9F09fC4cE57
│       └── [21:86] ECDSA (65 bytes):
│           ├── r: 0x94f8df9bcc3e2fa2049519666e9977ff76f9c99322db6a1f1117f3955411b2ae
│           ├── s: 0x316b72e49bd1743a5dee905ea4f27c4e7912479995f6b99eb56c44349dabe373
│           └── v: 28 (0x1c)
│
└── MAGIC SUFFIX (32 bytes): 0x6492649264926492649264926492649264926492649264926492649264926492
```

---

## 4. The message being signed in Aleph

Pyaleph builds the verification buffer in `src/aleph/chains/common.py`:

```python
buffer = f"{message.chain.value}\n{message.sender}\n{message.type.value}\n{message.item_hash}"
# Real example:
# "ETH\n0xa9F3Cd4E416c6e911DB3DcB5CA6CD77e9F861635\nPOST\n6f699b252db..."
```

It then wraps the buffer with the EIP-191 personal sign prefix in
`src/aleph/chains/evm.py`:

```python
message_hash = encode_defunct(text=verification.decode("utf-8"))
# Equivalent to eth_sign(buffer) = keccak256("\x19Ethereum Signed Message:\n" + len + buffer)
```

**Important:** the owner EOA (`0xfFFE…`) does **not** sign this hash directly. Kernel
wraps it in its own EIP-712 hash before presenting it to the EOA. This is why a direct
`ecrecover` over the inner ECDSA bytes returns `0x2eA6…` (an unrelated address) instead
of the owner `0xfFFE…`. The only correct verification path is to call the contract
itself (`isValidSignature`), which performs that wrapping internally.

---

## 5. Why signatures used to fail (two reasons)

### Reason A — ERC-6492 was not detected

`EVMVerifier.verify_signature` called `Account.recover_message(hash, signature)` on the
full 800+ byte blob. `eth-account` either raised or returned a meaningless address.

### Reason B — the owner does not sign the Aleph hash directly

Kernel wraps the message with its own EIP-712 domain before asking the owner to sign.
The only correct way to verify this is to call the contract (`isValidSignature`),
which performs the wrapping internally.

---

## 6. Example signature BEFORE deployment (ERC-6492)

The wallet `0xa9F3…1635` is NOT yet on chain. The signature carries the full ERC-6492
wrapper:

```
0x
# ABI-encoded (factory, calldata, innerSig):
000000000000000000000000d703aae79538628d27099b8c4f621be4ccd142d5  ← factory
0000000000000000000000000000000000000000000000000000000000000060  ← calldata offset
0000000000000000000000000000000000000000000000000000000000000260  ← innerSig offset
...452 bytes of factory calldata (createAccount + initialize)...
...86 bytes of inner signature (type + validator + r+s+v)...
6492649264926492649264926492649264926492649264926492649264926492  ← MAGIC (32 bytes)
```

**Validation requires:** simulate wallet deployment → call `isValidSignature`.

---

## 7. Example signature AFTER deployment (ERC-1271)

Once the wallet `0xa9F3…1635` has been deployed, Privy produces a signature without the
ERC-6492 wrapper. Only the 86 bytes of the inner signature are sent:

```
0x
01                                          ← type byte (root validator)
845adb2c711129d4f3966735ed98a9f09fc4ce57    ← validator plugin (20 bytes)
94f8df9bcc3e2fa2049519666e9977ff76f9c993    ← ECDSA r (partial, 32 bytes)
...
1c                                          ← ECDSA v
```

Full hex example (86 bytes):

```
0x01845adb2c711129d4f3966735ed98a9f09fc4ce5794f8df9bcc3e2fa2049519666e9977ff76f9c99322db6a1f1117f3955411b2ae316b72e49bd1743a5dee905ea4f27c4e7912479995f6b99eb56c44349dabe3731c
```

**Validation:** call `isValidSignature(hash, sig)` directly on the deployed contract.

---

## 8. Correct validation flow

```
┌─────────────────────────────────────────────────────────┐
│ Does the signature end with 0x6492…6492 (ERC-6492)?     │
└─────────────────────────────────────────────────────────┘
          │ Yes                             │ No
          ▼                                 ▼
┌─────────────────────────┐    ┌────────────────────────────┐
│ ERC-6492 (counterfactual)│   │ Is len(sig) == 65 bytes?   │
│ Decode (factory,         │   └────────────────────────────┘
│   calldata, innerSig)    │             │ Yes         │ No
│ Call UniversalSig        │             ▼             ▼
│   Validator via eth_call │  ┌──────────────┐  ┌──────────────┐
│ → isValidSignature?      │  │ Plain ECDSA  │  │ ERC-1271     │
└─────────────────────────┘   │ ecrecover    │  │ eth_call     │
         ▼                    │ == sender?   │  │ isValidSig   │
     valid/invalid            └──────────────┘  └──────────────┘
```

**Cost in RPC calls:**

- Plain ECDSA: 0 calls (cheapest)
- ERC-1271 deployed: 1 `eth_call`
- ERC-6492 counterfactual: 1 `eth_call` (via `UniversalSigValidator`)

---

## 9. ERC-1271 `isValidSignature`

Selector: `0x1626ba7e`

```python
from eth_abi.abi import encode

VALID_SIG_MAGIC = bytes.fromhex("1626ba7e")
IS_VALID_SIG_SELECTOR = bytes.fromhex("1626ba7e")

calldata = IS_VALID_SIG_SELECTOR + encode(
    ["bytes32", "bytes"], [message_hash, signature]
)

result = await w3.eth.call({"to": sender_address, "data": calldata})
is_valid = result[:4] == VALID_SIG_MAGIC
```

---

## 10. ERC-6492 `UniversalSigValidator`

ERC-6492 defines a `UniversalSigValidator` contract deployed at a deterministic address
on every major EVM chain. It validates counterfactual signatures in a single `eth_call`:

```python
from eth_abi.abi import encode

# UniversalSigValidator address (deterministic CREATE2)
# See: https://eips.ethereum.org/EIPS/eip-6492
UNIVERSAL_VALIDATOR = "0x0000000000002fd5Aeb385D324B580FCa7c83823"

calldata = encode(
    ["address", "bytes32", "bytes"],
    [sender_address, message_hash, full_erc6492_signature],
)

# Call with selector isValidSigWithSideEffects(address,bytes32,bytes) = 0x8dca4bea
selector = bytes.fromhex("8dca4bea")
result = await w3.eth.call({
    "to": UNIVERSAL_VALIDATOR,
    "data": selector + calldata,
})
is_valid = bool(int.from_bytes(result, "big"))
```

---

## 11. Current scope: Ethereum mainnet only

Smart wallet verification (ERC-1271 and ERC-6492) is intentionally limited to
`Chain.ETH` for now.

**Why:** smart wallets use `block.chainid` in their EIP-712 domain separator, so a
signature produced on (say) Base cannot be validated by calling `isValidSignature`
against an Ethereum mainnet RPC — the domains won't match and the call will return
invalid even when the signature is genuine. Until pyaleph has per-chain RPC URLs
wired through, other EVM chains keep their previous behavior: plain ECDSA only.

| Chain | Verifier | Plain ECDSA | ERC-1271 | ERC-6492 |
|---|---|---|---|---|
| `ETH` | `EthereumVerifier(rpc_url=...)` | ✅ | ✅ | ✅ |
| `ETHERLINK` | `EthereumVerifier()` (no RPC) | ✅ | ❌ (skipped) | ❌ (skipped) |
| All other EVM chains (Base, Arbitrum, Optimism, …) | `EVMVerifier()` (no RPC) | ✅ | ❌ (skipped) | ❌ (skipped) |

Plain EOA messages on every EVM chain continue to work exactly as before — the
scoping above affects only smart contract wallet signatures.

---

## 12. Files changed in pyaleph

| File | Change |
|---|---|
| `src/aleph/chains/evm.py` | Add ERC-6492 / ERC-1271 detection paths + RPC client |
| `src/aleph/chains/signature_verifier.py` | Pass `rpc_url` to `EVMVerifier` |
| `src/aleph/api_entrypoint.py` | Pass `config.ethereum.api_url.value` to `SignatureVerifier` |
| `src/aleph/jobs/process_pending_messages.py` | Same |
| `src/aleph/jobs/fetch_pending_messages.py` | Same |
| `tests/chains/test_evm.py` | Tests for ERC-1271 and ERC-6492 paths |

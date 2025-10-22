"""
Code imported from aleph-client to avoid a direct reference to the SDK.
"""

import hashlib
import logging
import struct
from binascii import hexlify, unhexlify
from typing import Optional

from coincurve.keys import PrivateKey, PublicKey

LOGGER = logging.getLogger(__name__)

PLACE_HOLDER = b"\xff\xff\xff\xff"
B58_DIGITS = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
MESSAGE_TEMPLATE = "\x18NULS Signed Message:\n{}"


class VarInt:
    # public final long value;
    # private final int originallyEncodedSize;

    def __init__(self, value=None):
        self.value = value
        self.originallyEncodedSize = 1
        if value is not None:
            self.originallyEncodedSize = self.getSizeInBytes()

    def parse(self, buf, offset):
        first = 0xFF & buf[offset]
        if first < 253:
            self.value = first
            # 1 data byte (8 bits)
            self.originallyEncodedSize = 1

        elif first == 253:
            self.value = (0xFF & buf[offset + 1]) | ((0xFF & buf[offset + 2]) << 8)
            # 1 marker + 2 data bytes (16 bits)
            self.originallyEncodedSize = 3

        elif first == 254:
            self.value = struct.unpack("<I", buf[offset + 1 : offset + 5])[0]
            # 1 marker + 4 data bytes (32 bits)
            self.originallyEncodedSize = 5

        else:
            self.value = struct.unpack("<Q", buf[offset + 1 : offset + 9])[0]
            # 1 marker + 8 data bytes (64 bits)
            self.originallyEncodedSize = 9

    def getOriginalSizeInBytes(self):
        return self.originallyEncodedSize

    def getSizeInBytes(self):
        return self.sizeOf(self.value)

    @classmethod
    def sizeOf(cls, value):
        # if negative, it's actually a very large unsigned long value
        if value < 0:
            # 1 marker + 8 data bytes
            return 9

        if value < 253:
            # 1 data byte
            return 1

        if value <= 0xFFFF:
            # 1 marker + 2 data bytes
            return 3

        if value <= 0xFFFFFFFF:
            # 1 marker + 4 data bytes
            return 5

        # 1 marker + 8 data bytes
        return 9

    # //    /**
    # //     * Encodes the value into its minimal representation.
    # //     *
    # //     * @return the minimal encoded bytes of the value
    # //     */
    def encode(self):
        size = self.sizeOf(self.value)

        if size == 1:
            return bytes((self.value,))
        elif size == 3:
            return bytes((253, self.value & 255, self.value >> 8))
        elif size == 5:
            return bytes((254,)) + struct.pack("<I", self.value)
        else:
            return bytes((255,)) + struct.pack("<Q", self.value)


def read_by_length(buffer, cursor=0, check_size=True):
    if check_size:
        fc = VarInt()
        fc.parse(buffer, cursor)
        length = fc.value
        size = fc.originallyEncodedSize
    else:
        length = buffer[cursor]
        size = 1

    value = buffer[cursor + size : cursor + size + length]
    return size + length, value


def write_with_length(buffer):
    if len(buffer) < 253:
        return bytes([len(buffer)]) + buffer
    else:
        return VarInt(len(buffer)).encode() + buffer


def getxor(body):
    xor = 0
    for c in body:
        xor ^= c
    return xor


def b58_encode(b):
    """Encode bytes to a base58-encoded string"""

    # Convert big-endian bytes to integer
    n = int("0x0" + hexlify(b).decode("utf8"), 16)

    # Divide that integer into bas58
    res = []
    while n > 0:
        n, r = divmod(n, 58)
        res.append(B58_DIGITS[r])
    res = "".join(res[::-1])

    # Encode leading zeros as base58 zeros
    czero = 0
    pad = 0
    for c in b:
        if c == czero:
            pad += 1
        else:
            break

    return B58_DIGITS[0] * pad + res


def b58_decode(s):
    """Decode a base58-encoding string, returning bytes"""
    if not s:
        return b""

    # Convert the string to an integer
    n = 0
    for c in s:
        n *= 58
        if c not in B58_DIGITS:
            raise ValueError(f"Character '{c}' is not a valid base58 character")
        digit = B58_DIGITS.index(c)
        n += digit

    # Convert the integer to bytes
    h = "%x" % n
    if len(h) % 2:
        h = "0" + h
    res = unhexlify(h.encode("utf8"))

    # Add padding back.
    pad = 0
    for c in s[:-1]:
        if c == B58_DIGITS[0]:
            pad += 1
        else:
            break

    return b"\x00" * pad + res


def address_from_hash(addr):
    return b58_encode(addr + bytes((getxor(addr),)))


def hash_from_address(hash):
    return b58_decode(hash)[:-1]


def public_key_to_hash(pub_key, chain_id=8964, address_type=1):
    sha256_digest = hashlib.sha256(pub_key).digest()
    md160_digest = hashlib.new("ripemd160", sha256_digest).digest()
    computed_address = (
        bytes(struct.pack("h", chain_id)) + bytes([address_type]) + md160_digest
    )
    return computed_address


class BaseNulsData:
    def _prepare(self, item):
        if item is None:
            return PLACE_HOLDER
        else:
            return item.serialize()


class NulsSignature(BaseNulsData):
    ALG_TYPE = 0  # only one for now...
    pub_key: Optional[bytes]
    digest_bytes: Optional[bytes]
    sig_ser: Optional[bytes]
    ecc_type: Optional[bytes]

    def __init__(self, data=None):
        self.pub_key = None
        self.digest_bytes = None
        self.sig_ser = None
        self.ecc_type = None
        if data is not None:
            self.parse(data)

    def __eq__(self, other):
        return all(
            (
                (self.pub_key == other.pub_key),
                (self.digest_bytes == other.digest_bytes),
                (self.sig_ser == other.sig_ser),
                (self.ecc_type == other.ecc_type),
            )
        )

    def parse(self, buffer, cursor=0):
        pos, self.pub_key = read_by_length(buffer, cursor)
        cursor += pos
        self.ecc_type = buffer[cursor]
        cursor += 1
        pos, self.sig_ser = read_by_length(buffer, cursor)
        cursor += pos
        return cursor

    @classmethod
    def sign_data(cls, pri_key: bytes, digest_bytes: bytes):
        privkey = PrivateKey(pri_key)
        # we expect to have a private key as bytes. unhexlify it before passing.
        item = cls()
        item.pub_key = privkey.public_key.format()
        item.digest_bytes = digest_bytes
        item.sig_ser = privkey.sign(digest_bytes, hasher=None)
        return item

    @classmethod
    async def sign_message(cls, pri_key: bytes, message):
        # we expect to have a private key as bytes. unhexlify it before passing
        privkey = PrivateKey(pri_key)
        item = cls()
        message = VarInt(len(message)).encode() + message
        item.pub_key = privkey.public_key.format()
        # item.digest_bytes = digest_bytes
        item.sig_ser = privkey.sign(MESSAGE_TEMPLATE.format(message).encode())
        return item

    def serialize(self, with_length=False):
        output = b""
        output += write_with_length(self.pub_key)
        output += bytes([0])  # alg ecc type
        output += write_with_length(self.sig_ser)
        if with_length:
            return write_with_length(output)
        else:
            return output

    def verify(self, message):
        pub = PublicKey(self.pub_key)
        message = VarInt(len(message)).encode() + message
        # LOGGER.debug("Comparing with %r" % (MESSAGE_TEMPLATE.format(message).encode()))
        try:
            if self.sig_ser is None:
                raise TypeError("sig_ser is None")
            good = pub.verify(self.sig_ser, MESSAGE_TEMPLATE.format(message).encode())
        except Exception:
            LOGGER.exception("Verification failed")
            good = False
        return good

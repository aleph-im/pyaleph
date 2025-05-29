import asyncio
from hashlib import sha256
from typing import Optional, Union

from aleph_message.models import ItemHash, ItemType

from aleph.exceptions import UnknownHashError
from aleph.settings import settings
from aleph.types.files import FileTag


async def run_in_executor(executor, func, *args):
    if settings.use_executors:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, func, *args)
    else:
        return func(*args)


def item_type_from_hash(item_hash: str) -> ItemType:
    # https://docs.ipfs.io/concepts/content-addressing/#identifier-formats
    if item_hash.startswith("Qm") and 44 <= len(item_hash) <= 46:  # CIDv0
        return ItemType.ipfs
    elif item_hash.startswith("bafy") and len(item_hash) == 59:  # CIDv1
        return ItemType.ipfs
    elif len(item_hash) == 64:
        return ItemType.storage
    else:
        raise UnknownHashError(f"Unknown hash {len(item_hash)} {item_hash}")


def get_sha256(content: Union[str, bytes]) -> str:
    if isinstance(content, str):
        content = content.encode("utf-8")
    return sha256(content).hexdigest()


def safe_getattr(obj, attr, default=None):
    for part in attr.split("."):
        obj = getattr(obj, part, default)
        if obj is default:
            break
    return obj


def make_file_tag(owner: str, ref: Optional[str], item_hash: str) -> FileTag:
    """
    Builds the file tag corresponding to a STORE message.

    The file tag can be set to two different values:
    * if the `ref` field is not set, the tag will be set to <item_hash>.
    * if the `ref` field is set, two cases: if `ref` is an item hash, the tag is
      the value of the ref field. If it is a user-defined value, the tag is
      <owner>/<ref>.

    :param owner: Owner of the file.
    :param ref: Value of the `ref` field of the message content.
    :param item_hash: Item hash of the message.
    :return: The computed file tag.
    """

    # When the user does not specify a ref, we use the item hash.
    if ref is None:
        return FileTag(item_hash)

    # If ref is an item hash, return it as is
    try:
        _item_hash = ItemHash(ref)
        return FileTag(ref)
    except ValueError:
        pass

    return FileTag(f"{owner}/{ref}")

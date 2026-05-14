#!/usr/bin/env python3
"""Mark processed messages as rejected.

Use when a message was accepted under permissive validation rules that have
since become stricter (ex: BaseExecutableContent.metadata now requires a dict
and rejects lists, but some nodes accepted such messages historically). The
API returns 500 on those messages because parsed_content access raises; moving
them to the rejected state matches what nodes that rejected them in the first
place expose to clients.

The actual rejection logic lives in `aleph.repair.mark_processed_message_as_rejected`
and is also wired into `repair_node`, which runs at startup. This script
exists for ad-hoc cleanups when you have a known list of hashes and don't
want to wait for the next restart.

Runs as a dry-run by default. Pass --commit to actually persist changes.
Hashes can be provided via repeated --hash flags or via --hashes-file (one
hash per line, lines starting with # are skipped).
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from sqlalchemy import select  # noqa: E402

import aleph.config  # noqa: E402
from aleph.db.connection import make_engine, make_session_factory  # noqa: E402
from aleph.db.models.messages import MessageDb  # noqa: E402
from aleph.repair import mark_processed_message_as_rejected  # noqa: E402
from aleph.types.db_session import DbSession  # noqa: E402
from aleph.types.message_status import ErrorCode, MessageStatus  # noqa: E402

LOGGER = logging.getLogger("reject_processed_messages")


def reject_processed_message(
    session: DbSession,
    item_hash: str,
    error_code: ErrorCode,
    reason: str,
) -> bool:
    message = session.execute(
        select(MessageDb).where(MessageDb.item_hash == item_hash)
    ).scalar_one_or_none()

    if message is None:
        LOGGER.warning("%s: not found in messages, skipping", item_hash)
        return False

    if message.status_value == MessageStatus.REJECTED:
        LOGGER.info("%s: already rejected, skipping", item_hash)
        return False

    if message.status_value != MessageStatus.PROCESSED:
        LOGGER.warning(
            "%s: unexpected status %s, skipping",
            item_hash,
            message.status_value,
        )
        return False

    message_type = message.type
    mark_processed_message_as_rejected(
        session=session,
        message=message,
        error_code=error_code,
        reason=reason,
    )

    LOGGER.info(
        "%s: rejected (type=%s, error_code=%s)",
        item_hash,
        message_type,
        error_code.name,
    )
    return True


def _read_hashes(args: argparse.Namespace) -> List[str]:
    hashes: List[str] = list(args.hash or [])
    if args.hashes_file:
        with open(args.hashes_file, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if line and not line.startswith("#"):
                    hashes.append(line)
    return hashes


def _parse_error_code(value: str) -> ErrorCode:
    if value.lstrip("-").isdigit():
        return ErrorCode(int(value))
    return ErrorCode[value]


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", dest="config_file", default=None, help="Config file path"
    )
    parser.add_argument(
        "--hash",
        action="append",
        default=[],
        help="Message item hash to reject. Pass multiple times for several hashes.",
    )
    parser.add_argument(
        "--hashes-file",
        default=None,
        help="Path to a file with one item hash per line.",
    )
    parser.add_argument(
        "--error-code",
        type=_parse_error_code,
        default=ErrorCode.INVALID_FORMAT,
        help="ErrorCode to record (name or integer, default: INVALID_FORMAT).",
    )
    parser.add_argument(
        "--reason",
        default=(
            "Marked rejected by reject_processed_messages.py: content fails "
            "validation under current rules."
        ),
        help="Free-text reason stored on the rejected_messages row.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist changes. Without this flag the script runs as a dry-run.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging."
    )

    args = parser.parse_args(list(argv))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    hashes = _read_hashes(args)
    if not hashes:
        parser.error("Provide --hash and/or --hashes-file with at least one hash")

    config = aleph.config.app_config
    if args.config_file is not None:
        config.yaml.load(args.config_file)

    engine = make_engine(config=config, application_name="reject-processed-messages")
    session_factory = make_session_factory(engine)

    changed = 0
    skipped = 0
    errors = 0

    for item_hash in hashes:
        with session_factory() as session:
            try:
                applied = reject_processed_message(
                    session=session,
                    item_hash=item_hash,
                    error_code=args.error_code,
                    reason=args.reason,
                )
            except Exception:
                LOGGER.exception("%s: failed to reject", item_hash)
                session.rollback()
                errors += 1
                continue

            if not applied:
                session.rollback()
                skipped += 1
                continue

            if args.commit:
                session.commit()
                changed += 1
            else:
                session.rollback()
                LOGGER.info("%s: dry-run, rolled back", item_hash)
                changed += 1

    mode = "commit" if args.commit else "dry-run"
    LOGGER.info(
        "Done [%s]: %d changed, %d skipped, %d errors",
        mode,
        changed,
        skipped,
        errors,
    )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

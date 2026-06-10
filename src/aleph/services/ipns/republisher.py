"""
Periodic IPNS maintenance: republish stored records to the DHT (kubo
records expire within ~24-48h) and re-resolve names to adopt newer
records published out-of-band or by tracked third parties.
"""

import asyncio
import datetime as dt
import logging

from configmanager import Config

from aleph.db.accessors.files import (
    get_ipns_file_pin,
    insert_grace_period_file_pin,
    is_pinned_file,
    update_ipns_file_pin,
    upsert_file,
)
from aleph.db.accessors.ipns import get_all_ipns_records
from aleph.db.models.ipns import IpnsRecordDb
from aleph.handlers.content.store import _get_file_stats_from_ipfs
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.service import InvalidIpnsRecordError, IpnsResolutionError
from aleph.toolkit.constants import MiB
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.ipns import IpnsStatus

LOGGER = logging.getLogger(__name__)


class IpnsRepublisher:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        ipfs_service: IpfsService,
        grace_period_hours: int,
        stat_timeout: int,
        resolve_timeout: int,
    ):
        self.session_factory = session_factory
        self.ipfs_service = ipfs_service
        self.grace_period_hours = grace_period_hours
        self.stat_timeout = stat_timeout
        self.resolve_timeout = resolve_timeout

    async def _republish(self, record_db: IpnsRecordDb) -> None:
        now = utc_now()
        if record_db.record_validity is not None and record_db.record_validity <= now:
            if record_db.status != IpnsStatus.EXPIRED:
                LOGGER.info("ipns_republish name=%s outcome=expired", record_db.name)
                record_db.status = IpnsStatus.EXPIRED
            return
        if record_db.record is None:
            return
        try:
            await self.ipfs_service.put_ipns_record(record_db.name, record_db.record)
            record_db.last_republished = now
        except Exception:
            LOGGER.warning(
                "ipns_republish name=%s outcome=fail", record_db.name, exc_info=True
            )

    async def _re_resolve(self, session: DbSession, record_db: IpnsRecordDb) -> None:
        try:
            record = await self.ipfs_service.resolve_ipns_record(
                record_db.name, timeout=self.resolve_timeout
            )
            record_info = await self.ipfs_service.verify_ipns_record(
                record, record_db.name
            )
        except (IpnsResolutionError, InvalidIpnsRecordError):
            return

        if (
            record_db.record_sequence is not None
            and record_info.sequence <= record_db.record_sequence
        ):
            return

        file_stats = await _get_file_stats_from_ipfs(
            cid=record_info.value_cid,
            ipfs_service=self.ipfs_service,
            stat_timeout=self.stat_timeout,
        )
        if file_stats.size > record_db.max_size_mib * MiB:
            # Keep serving the last good CID; store the newer record so the
            # sequence guard and republishing stay current.
            LOGGER.info(
                "ipns_resolve name=%s sequence=%d outcome=over_quota",
                record_db.name,
                record_info.sequence,
            )
            record_db.record = record
            record_db.record_sequence = record_info.sequence
            record_db.record_validity = record_info.validity
            record_db.status = IpnsStatus.OVER_QUOTA
            return

        await self.ipfs_service.pin_add(cid=record_info.value_cid)
        upsert_file(
            session=session,
            file_hash=record_info.value_cid,
            file_type=file_stats.file_type,
            size=file_stats.size,
        )

        old_cid = record_db.resolved_cid
        record_db.record = record
        record_db.record_sequence = record_info.sequence
        record_db.record_validity = record_info.validity
        record_db.resolved_cid = record_info.value_cid
        record_db.last_resolved = utc_now()
        record_db.status = IpnsStatus.OK

        pin = get_ipns_file_pin(session, name=record_db.name, owner=record_db.owner)
        if pin is not None and pin.file_hash != record_info.value_cid:
            update_ipns_file_pin(
                session=session,
                name=record_db.name,
                owner=record_db.owner,
                file_hash=record_info.value_cid,
                item_hash=pin.item_hash,
            )
        if old_cid and old_cid != record_info.value_cid:
            session.flush()
            if not is_pinned_file(session=session, file_hash=old_cid):
                delete_by = utc_now() + dt.timedelta(hours=self.grace_period_hours)
                insert_grace_period_file_pin(
                    session=session,
                    file_hash=old_cid,
                    created=utc_now(),
                    delete_by=delete_by,
                )

    async def run_cycle(self) -> None:
        with self.session_factory() as session:
            records = list(get_all_ipns_records(session))
            for record_db in records:
                await self._republish(record_db)
                await self._re_resolve(session, record_db)
            session.commit()


async def ipns_republisher_task(config: Config, republisher: IpnsRepublisher) -> None:
    period = config.ipfs.ipns.republish_period_hours.value * 3600
    while True:
        try:
            LOGGER.info("Next IPNS republish cycle in %.0f seconds.", period)
            await asyncio.sleep(period)
            LOGGER.info("Starting IPNS republish cycle...")
            await republisher.run_cycle()
            LOGGER.info("IPNS republish cycle completed.")
        except Exception:
            LOGGER.exception("Error in IPNS republish cycle")

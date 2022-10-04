import datetime as dt
from typing import Optional

from aleph_message.models import Chain
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from aleph.types.db_session import DbSession
from ..models.chains import ChainSyncStatusDb


def get_last_height(session: DbSession, chain: Chain) -> Optional[int]:
    height = (
        session.execute(
            select(ChainSyncStatusDb.height).where(ChainSyncStatusDb.chain == chain)
        )
    ).scalar()
    return height


def upsert_chain_sync_status(
    session: DbSession,
    chain: Chain,
    height: int,
    update_datetime: dt.datetime,
) -> None:
    upsert_stmt = (
        insert(ChainSyncStatusDb)
        .values(chain=chain, height=height, last_update=update_datetime)
        .on_conflict_do_update(
            constraint="chains_sync_status_pkey",
            set_={"height": height, "last_update": update_datetime},
        )
    )
    session.execute(upsert_stmt)

"""``INFINITY`` sentinel for the credit_balances expiration column.

The bucket cache models "no expiration" as a real timestamptz with PG's
``infinity`` value, not NULL. That keeps the composite primary key columns
NOT NULL and lets every read query use the same ``expiration_date > NOW()``
shape without an ``IS NULL`` branch.

The Python side uses a ``datetime`` subclass so we can:

  * register a psycopg2 adapter that intercepts just this sentinel and
    emits ``'infinity'::timestamptz`` on the wire; regular ``datetime``
    values keep the default adapter; and
  * register a typecaster that decodes PG ``infinity::timestamptz`` back to
    an ``_Infinity`` instance (rather than a plain ``datetime(9999, ...)``)
    so subsequent ORM round-trips serialise it as PG ``infinity`` again.
    Without the typecaster the ORM would track a regular datetime for the
    PK and re-serialise it as a year-9999 literal, which is *not* equal to
    PG ``infinity`` at the SQL level and would cause UPDATE WHERE PK to
    miss the row (StaleDataError).

Round-trip assumption: SQLAlchemy's ``DateTime`` result processor for the
psycopg2 dialect is a passthrough, so the ``_Infinity`` subclass produced
by our typecaster reaches the ORM unchanged and the outbound adapter sees
``_Infinity`` (not a normalised ``datetime``) on the UPDATE path. If a
future SQLAlchemy version starts coercing timestamptz values during result
processing, the symptom would be loud (StaleDataError on bucket UPDATE),
not silent corruption; the fallback would be to expose this sentinel via a
custom ``TypeDecorator`` that coerces year-9999/us-999999 values back to
``_Infinity`` on load.
"""

import datetime as dt

import psycopg2.extensions

_TIMESTAMPTZ_OID = 1184  # pg_type.oid for timestamptz


class _Infinity(dt.datetime):
    """``datetime`` subclass whose only canonical instance is ``INFINITY``.

    Subclassing ``datetime`` lets the value participate in normal comparison
    and arithmetic with other datetimes (it sorts after every representable
    timestamptz), while remaining a distinct type for adapter dispatch.
    """

    __slots__ = ()

    def __new__(cls) -> "_Infinity":
        # Matches what psycopg2 returns when decoding PG ``infinity::timestamptz``
        # so reads from the DB compare equal to this sentinel via datetime equality.
        return dt.datetime.__new__(
            cls, 9999, 12, 31, 23, 59, 59, 999999, tzinfo=dt.timezone.utc
        )

    def __repr__(self) -> str:
        return "INFINITY"


INFINITY: _Infinity = _Infinity()


def _adapt_infinity(_value: _Infinity) -> psycopg2.extensions.AsIs:
    return psycopg2.extensions.AsIs("'infinity'::timestamptz")


_DEFAULT_TIMESTAMPTZ = psycopg2.extensions.PYDATETIMETZ


def _cast_timestamptz(value, cur):
    """Replacement typecaster for OID 1184 that decodes PG ``infinity`` to ``INFINITY``."""
    if value == "infinity":
        return INFINITY
    return _DEFAULT_TIMESTAMPTZ(value, cur)


_INFINITY_AWARE_TIMESTAMPTZ = psycopg2.extensions.new_type(
    (_TIMESTAMPTZ_OID,),
    "TIMESTAMPTZ_INFINITY",
    _cast_timestamptz,
)


_registered = False


def register_infinity_adapter() -> None:
    """Register psycopg2 to round-trip ``INFINITY`` <-> PG ``infinity::timestamptz``.

    * Outbound: ``_Infinity`` instances serialise to ``'infinity'::timestamptz``.
    * Inbound: ``timestamptz infinity`` values decode to ``INFINITY`` (the
      ``_Infinity`` singleton), so the ORM tracks a value that re-serialises
      as PG ``infinity`` on subsequent UPDATE statements.

    Idempotent. Safe to call at module-import time before any connections
    are opened; adapters and typecasters are global to the psycopg2 module.
    """
    global _registered
    if _registered:
        return
    psycopg2.extensions.register_adapter(_Infinity, _adapt_infinity)
    psycopg2.extensions.register_type(_INFINITY_AWARE_TIMESTAMPTZ)
    _registered = True

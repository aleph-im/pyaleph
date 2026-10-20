import pytest
from pydantic import ValidationError

from aleph.schemas.execution_intervals import ExecutionIntervalsContent


def _content(start: int) -> dict:
    return {
        "version": 1,
        "period": {"start": start, "end": start + 3600},
        "source": "s",
        "intervals": [],
        "totals": {},
    }


def test_valid_period_accepted():
    ExecutionIntervalsContent.model_validate(
        _content(1_700_000_400 - 1_700_000_400 % 3600)
    )


@pytest.mark.parametrize("start", [3600 * 10**15, -3600, 4_102_444_800 + 3600])
def test_out_of_range_period_start_rejected(start):
    with pytest.raises(ValidationError):
        ExecutionIntervalsContent.model_validate(_content(start))


def test_intervals_capped_at_max():
    from aleph.schemas.execution_intervals import MAX_INTERVALS

    start = 1_700_000_400 - 1_700_000_400 % 3600
    entry = {"vm": "v", "node": "n", "start": start, "end": start + 1}
    content = _content(start)
    content["intervals"] = [entry] * MAX_INTERVALS
    ExecutionIntervalsContent.model_validate(content)
    content["intervals"] = [entry] * (MAX_INTERVALS + 1)
    with pytest.raises(ValidationError):
        ExecutionIntervalsContent.model_validate(content)

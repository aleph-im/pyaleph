from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator

PERIOD_SECONDS = 3600
MAX_PERIOD_START = 4_102_444_800  # 2100-01-01 UTC, keeps fromtimestamp() safe
MAX_INTERVALS = 50_000


class Period(BaseModel):
    start: StrictInt
    end: StrictInt


class IntervalEntry(BaseModel):
    vm: str
    node: str
    start: StrictInt
    end: StrictInt

    @field_validator("vm", "node")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty")
        return v


class ExecutionIntervalsContent(BaseModel):
    version: Literal[1]
    period: Period
    source: str
    events_cursor: Optional[Dict[str, Any]] = None
    # Bounded so that one post cannot make every node's fold load an unbounded
    # list; far above the number of VMs the network runs in an hour.
    intervals: List[IntervalEntry] = Field(max_length=MAX_INTERVALS)
    totals: Dict[str, Any]
    supersedes: Optional[str] = None

    @model_validator(mode="after")
    def check_period_and_intervals(self) -> "ExecutionIntervalsContent":
        period = self.period
        if period.end - period.start != PERIOD_SECONDS:
            raise ValueError("period must span exactly one hour")
        if not 0 <= period.start <= MAX_PERIOD_START:
            raise ValueError("period.start is out of range")
        if period.start % PERIOD_SECONDS != 0:
            raise ValueError("period.start must be aligned on the hour")
        for interval in self.intervals:
            if not (period.start <= interval.start < interval.end <= period.end):
                raise ValueError(
                    f"interval of vm {interval.vm} is outside the period "
                    "or has end <= start"
                )
        return self

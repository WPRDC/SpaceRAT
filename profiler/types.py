import datetime
from typing import TypedDict, Any, NotRequired, Literal


class QuestionResultsRow(TypedDict):
    time: datetime.datetime
    value: NotRequired[Any]
    mean: NotRequired[float]
    mode: NotRequired[Any]
    min: NotRequired[float | int]
    first_quartile: NotRequired[float | int]
    median: NotRequired[float | int]
    third_quartile: NotRequired[float | int]
    max: NotRequired[float | int]
    sum: NotRequired[float | int]


SourceType = Literal[
    "census",
    "acs",
    "datastore",
]


TemporalResolution = Literal[
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "decade",
    "century",
    "millennium",
]

TemporalDomain = Literal[
    "current",
    # past: from the period ago until now
    "past-minute",
    "past-hour",
    "past-day",
    "past-week",
    "past-month",
    "past-quarter",
    "past-year",
    "past-decade",
    # last: last full cycle of the period
    "last-hour",
    "last-day",
    "last-week",
    "last-month",
    "last-year",
]


DataType = Literal[
    "continuous",
    "discrete",
]

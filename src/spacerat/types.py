import datetime
from typing import TypedDict, Literal


class RecordResultsRow(TypedDict):
    time: datetime.datetime
    region: str
    parent_id: str
    # todo: handle arbitrary fields -
    #   remaining fields are based on questions (e.g. fair_market_assessed_value__mean, etc.)


class AggregateResultsRow(TypedDict):
    time: datetime.datetime
    region: str

    # todo: handle arbitrary fields -
    #   remaining fields are based on questions (e.g. fair_market_assessed_value__mean, etc.)
    # value: NotRequired[Any]
    # mean: NotRequired[float]
    # mode: NotRequired[Any]
    # min: NotRequired[float | int]
    # first_quartile: NotRequired[float | int]
    # median: NotRequired[float | int]
    # third_quartile: NotRequired[float | int]
    # max: NotRequired[float | int]
    # sum: NotRequired[float | int]


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


DataType = Literal["continuous", "discrete", "boolean", "date"]

# possible formats to use on frontend for values
ValueFormat = Literal[
    "number",  # user's locale number format (e.g. en-US: 1,337.01)
    "raw",  # raw string of value (good for years)
    "money",  # user's local currency format (e.g. en-US: $1,337.00)
    "date",  # user's locale data format (e.g. en-US: 1/13/1999)
    "datetime",  # user's locale datetime form (e.g. en-US: 4/30/2005, 1:33:21 PM)
    "isodatetime",  # prints iso datetime format (e.g. 2005-04-30)
    "scientific",  # 🧪
]

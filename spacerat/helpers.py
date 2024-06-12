from datetime import timedelta
from typing import Iterable

from spacerat.types import QuestionResultsRow, DataType


def print_records(records: Iterable[QuestionResultsRow]) -> None:
    for record in records:
        print_record(record)


def print_record(record: QuestionResultsRow) -> None:
    print(record["time"].isoformat())
    for k, v in record.items():
        if k == "time":
            continue
        print(f"  - {k}: {v}")


def parse_period_name(period_name: str) -> timedelta:
    """Convert time period name to timedelta"""
    # todo: handle calendar periods more rigorously
    if period_name in ["minute", "hour", "day"]:
        return timedelta(**{f"{period_name}s": 1})
    elif period_name == "week":
        return timedelta(days=7)
    elif period_name == "month":
        return timedelta(days=30)
    elif period_name == "quarter":
        return 3 * timedelta(days=30)
    elif period_name == "year":
        return timedelta(days=365)
    elif period_name == "decade":
        return 10 * timedelta(days=365.25)

    raise ValueError("Invalid time period.")


def get_aggregate_fields(datatype: DataType) -> str:
    if datatype == "continuous":
        return """
          AVG(value)                                            as mean,
          MODE() WITHIN GROUP (ORDER BY value)                  as mode,

          MIN(value)                                            as min,
          percentile_cont(0.25) WITHIN GROUP ( ORDER BY value ) as first_quartile,
          percentile_cont(0.5) WITHIN GROUP ( ORDER BY value )  as median,
          percentile_cont(0.75) WITHIN GROUP ( ORDER BY value ) as third_quartile,
          MAX(value)                                            as max,

          stddev_pop(value)                                     as stddev,

          SUM(value)                                            as sum,
          COUNT(*)                                              as n
        """

    # discrete can only do mode and count
    return """
      MODE() WITHIN GROUP (ORDER BY value) as mode,
      COUNT(value)                         as n
    """

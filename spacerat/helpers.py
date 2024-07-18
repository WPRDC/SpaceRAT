import typing
from datetime import timedelta
from typing import Iterable

if typing.TYPE_CHECKING:
    from spacerat.model import Geography, Question
    from spacerat.types import QuestionResultsRow, DataType


def print_records(records: Iterable["QuestionResultsRow"]) -> None:
    for record in records:
        print_record(record)


def print_record(record: "QuestionResultsRow") -> None:
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


def get_aggregate_fields(question: "Question") -> str:
    field_name = question.field_name
    if question.datatype == "continuous":
        return f"""
          AVG({field_name})                                          as {field_name}__mean,
          MODE() WITHIN GROUP (ORDER BY {field_name})                as {field_name}__mode,

          MIN({field_name})                                          as {field_name}__min,
          percentile_cont(0.25) WITHIN GROUP (ORDER BY {field_name}) as {field_name}__first_quartile,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY {field_name})  as {field_name}__median,
          percentile_cont(0.75) WITHIN GROUP (ORDER BY {field_name}) as {field_name}__third_quartile,
          MAX({field_name})                                          as {field_name}__max,

          stddev_pop({field_name})                                   as {field_name}__stddev,

          SUM({field_name})                                          as {field_name}__sum,
          COUNT(*)                                                   as {field_name}__n
        """

    # discrete can only do mode and count
    return f"""
      MODE() WITHIN GROUP (ORDER BY {field_name}) as mode,
      COUNT(*)                                    as n
    """


def get_variant_clause(
    subgeog: "Geography",
    variant: str = None,
) -> str:
    # optionally, get extra clause to filter for variant
    variant_clause = None
    if variant is not None and variant in subgeog.variants:
        variant_clause = subgeog.variants[variant].where_clause

    if variant_clause:
        variant_clause = "AND " + variant_clause
    else:
        variant_clause = ""

    return variant_clause


def as_field_name(fid: str) -> str:
    return fid.replace("-", "_").strip()

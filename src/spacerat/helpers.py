import os
from datetime import timedelta
from typing import Iterable
from typing import Literal, TYPE_CHECKING

from slugify import slugify

if TYPE_CHECKING:
    from spacerat.models import Question
    from spacerat.types import AggregateResultsRow


MARTIN_URL = os.environ.get("SPACERAT_MARTIN_URL", "http://localhost:3000")


def print_records(records: Iterable["AggregateResultsRow"]) -> None:
    for record in records:
        print_record(record)


def print_record(
    record: "AggregateResultsRow",
    group_by: Literal["time", "region"] = "region",
) -> None:
    print(record[group_by])
    for k, v in record.items():
        if k == group_by:
            continue
        print(f"  - {k}: {v}")


def by_region(records: list["AggregateResultsRow"]) -> dict:
    results: dict = {}
    for record in records:
        if record["region"] not in results:
            results[record["region"]] = {}

        for key, value in record.items():
            parts = key.split("__")

            if len(parts) > 1:
                field, stat = parts[0], parts[1]
                if field not in results[record["region"]]:
                    results[record["region"]][field] = {}

                results[record["region"]][field][stat] = value
            else:
                results[record["region"]][key] = value
    return results


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
    """
    Creates a string of aggregate select statements for use in top-level of main query.
    :param question:
    :return:
    """
    field_name = question.field_name
    # continuous works for continuous values
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

    # date types have some limits right now
    if question.datatype == "date":
        return f"""
          MIN({field_name})                                          as {field_name}__min,
          MAX({field_name})                                          as {field_name}__max,
          COUNT(*)                                                   as {field_name}__n
        """

    # boolean datatypes are count of true
    if question.datatype == "boolean":
        return f"""
          COUNT(*) FILTER (WHERE {field_name})  as {field_name}__count,
          (COUNT(*) FILTER (WHERE {field_name})::float / COUNT(*)::float) as {field_name}__percent,
          COUNT(*)                              as {field_name}__n
        """

    # discrete can only do mode and count
    return f"""
      MODE() WITHIN GROUP (ORDER BY {field_name}) as {field_name}__mode,
      COUNT(*)                                    as {field_name}__n
    """


def get_subgeog_clause(
    clause_options: dict,
    clause_id: str = None,
) -> str | None:
    # optionally, get extra clause to filter for variant
    if clause_id is not None and clause_id in clause_options:
        return clause_options[clause_id].where_clause

    return None


def as_field_name(fid: str) -> str:
    return fid.replace("-", "_").strip()


def tbl_name(field: str) -> str:
    return slugify(field, separator="_")


def tileserver_url(map_table: str) -> str:
    return f'{MARTIN_URL.rstrip("/")}/table.{map_table}.geom'

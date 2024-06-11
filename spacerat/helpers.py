from datetime import timedelta

from spacerat.types import QuestionResultsRow


def print_record(record: QuestionResultsRow):
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

import os
from email.policy import default

from flask import Flask, request, abort
from flask_sqlalchemy import SQLAlchemy
from spacerat.core import QuestionParam, RegionParam, SpaceRAT
from spacerat.helpers import by_region
from spacerat.models import TimeAxis, Base

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("SPACERAT_DB_URL")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False


def parse_param(param: str) -> str or list[str]:
    if "," in param:
        return param.split(",")
    return param


def parse_bool(param: str) -> bool:
    return param.lower() == "true"


@app.route("/answer", methods=["GET"])
def answer():
    # Parse parameters
    question: QuestionParam = parse_param(request.args.get("question", ""))
    region: RegionParam = parse_param(request.args.get("region", ""))
    time_axis: TimeAxis | None = request.args.get("timeAxis")

    variant: str | None = request.args.get("variant")

    filter_: str | None = request.args.get("filter")
    filter_arg: str | None = request.args.get("filterArg")

    aggregate: bool = parse_bool(request.args.get("aggregate", "true"))
    query_records: bool = parse_bool(request.args.get("queryRecords", "false"))

    # Instantiate spacerat
    rat = SpaceRAT()

    # answer questions
    aggregate_stats, records = rat.answer_question(
        question=question,
        region=region,
        variant=variant,
        filter=filter_,
        filter_arg=filter_arg,
        aggregate=aggregate,
        query_records=query_records,
        geom="geojson",
    )

    response = {
        "results": {
            "stats": by_region(aggregate_stats),
            "records": records,
        }
    }

    return response


def _show_model(model_type: str, model_id: str):
    rat = SpaceRAT()
    if model_id:
        getter = getattr(rat, f"get_{model_type}")
        obj = getter(model_id)
        if obj:
            return {
                "results": obj.as_dict(),
            }
        else:
            abort(404, f"{model_type.capitalize()} not found")
    else:
        getter = getattr(rat, f"get_{model_type}s")
        objs = getter()
        return {
            "results": [obj.as_brief() for obj in objs],
        }


@app.route("/source/", defaults={"source_id": None})
@app.route("/source/<source_id>")
def show_source(source_id: str):
    return _show_model("source", source_id)


@app.route("/question/", defaults={"question_id": None})
@app.route("/question/<question_id>")
def show_question(question_id: str):
    return _show_model("question", question_id)


@app.route("/geography/", defaults={"geog_id": None})
@app.route("/geography/<geog_id>")
def show_geography(geog_id: str):
    return _show_model("geog", geog_id)


@app.route("/maps/", defaults={"map_id": None})
@app.route("/maps/<map_id>")
def show_maps(map_id: str):
    return _show_model("map_config", map_id)

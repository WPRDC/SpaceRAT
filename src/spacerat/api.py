import os

from flask import Flask, request, abort

from spacerat.core import QuestionParam, RegionParam, SpaceRAT
from spacerat.helpers import by_region
from spacerat.models import TimeAxis

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("SPACERAT_DB_URL")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config['APPLICATION_ROOT'] = os.environ.get("APPLICATION_ROOT")


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

    geom: bool = parse_bool(request.args.get("geom", "false"))

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
        geom="geojson" if geom else None,
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
                "results": [obj.as_dict()],
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


@app.route("/breaks/")
def get_breaks():
    try:
        mapset_id = request.args["mapset"]
        geog_level = request.args["geog"]
        question_id = request.args["question"]
        stat = request.args["stat"]
        variant = request.args.get("variant")
        n_classes = request.args.get("bin")

    except KeyError:
        abort(400, "mapset, geog, question, and stat parameters are required.")

    rat = SpaceRAT()

    return {
        "results": rat.calculate_breaks(
            mapset_id,
            geog_level,
            question_id,
            stat,
            variant=variant,
            n_classes=n_classes,
        ),
    }

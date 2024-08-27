import os
from base64 import urlsafe_b64encode
from pathlib import Path
from uuid import uuid4

import yaml
from ckanapi import RemoteCKAN
from slugify import slugify
from sqlalchemy.orm import Session

from spacerat import SpaceRAT
from spacerat.models import Source, Question, QuestionSource
from spacerat.types import DataType

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


ckan_url = os.environ.get("CKAN_URL", "https://data.wprdc.org")

ckan = RemoteCKAN(ckan_url)

IGNORED_FIELDS = [
    "_id",
    "_full_text",
    "_geom",
    "_centroid",
]


CONTINUOUS_TYPES = [
    # int
    "int",
    "int2",
    "int4",
    "int8",
    "smallint" "integer",
    "bigint",
    # float/decimal
    "float4",
    "float8",
    "real",
    "double precision",
    "numeric",
    "decimal",
    "money",
    # date/time
    "date",
    "time",
    "timestamp",
    "timetz",
    "timestamptz",
]

DISCRETE_TYPES = [
    "text",
    "char",
    "varchar" "character",
    "character varying",
    "uuid",
]


BOOLEAN_TYPES = ["boolean", "bool"]


QUESTION_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))).parent.parent.parent
    / "model"
    / "questions"
)


def _random_str():
    return str(uuid4()).split("-")[0]


def datatype_map(dtype: str) -> DataType:
    if dtype in CONTINUOUS_TYPES:
        return "continuous"
    if dtype in BOOLEAN_TYPES:
        return "boolean"
    return "discrete"


def generate_questions_for_source(source: "Source", rat: SpaceRAT):
    # get list of fields from the resource
    fields = ckan.action.datastore_search(id=source.table, limit=0)["fields"]

    # organize by source
    output_dir = QUESTION_DIR / source.id
    if not output_dir.exists():
        os.mkdir(output_dir)

    counter = 0
    for field in fields:
        # skip hidden fields
        if field["id"] in IGNORED_FIELDS:
            continue

        _id = slugify(field["id"].lower())

        # prevent duplicate IDs
        if rat.has_question(_id):
            print("Duplicate ID found:", _id, end="")
            _id += _random_str().lower()
            print(". using", _id)

        # load question into ontology
        with Session(rat.engine) as session:
            question = Question(
                id=_id,
                name=field["info"]["label"] if "info" in field else field["id"],
                datatype=datatype_map(field["type"]),
            )
            question_source = QuestionSource(
                source_id=source.id,
                geography_id=source.spatial_resolution,
                value_select=f'"{field["id"]}"',
            )
            question.sources.append(question_source)
            session.add_all([question, question_source])

        # dump question yaml
        with open(output_dir / f"{_id}.yaml", "w") as f:
            counter += 1
            f.write(question.as_yaml())

    return counter

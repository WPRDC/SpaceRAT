import os
from os import PathLike
from pathlib import Path
from typing import TypeVar, Type, Sequence

from sqlalchemy import Engine, create_engine, select, ColumnExpressionArgument
from sqlalchemy.orm import Session

from spacerat import db
from spacerat.config import init_db
from spacerat.helpers import get_aggregate_fields
from spacerat.model import Question, TimeAxis, Region, Geography, Source
from spacerat.types import QuestionResultsRow

DEFAULT_ENGINE = create_engine("sqlite://")
DEFAULT_MODEL_DIR = Path(os.getcwd()) / "model"

T = TypeVar("T")


class SpaceRAT:
    engine: Engine

    def __init__(
        self,
        engine: Engine = None,
        model_dir: PathLike = DEFAULT_MODEL_DIR,
        debug: bool = False,
    ) -> None:
        """
        Initialize a SpaceRAT instance.

        :param engine: SQLAlchemy engine to manage the SpaceRAT model. Defaults to in-memory sqlite engine.
        """
        self.engine = engine or DEFAULT_ENGINE
        if debug:
            self.engine.echo = True
        init_db(self.engine, model_dir)

    def get_source(self, sid: str) -> Source | None:
        """Returns the Source object with id `sid`"""
        return self._get_obj(Source, sid)

    def get_question(self, qid: str) -> Question | None:
        return self._get_obj(Question, qid)

    def get_geography(self, *args, **kwargs):
        """Alias for get_geog"""
        return self.get_geog(*args, **kwargs)

    def get_geog(self, gid: str) -> Geography | None:
        return self._get_obj(Geography, gid)

    def get_region(self, rid: str) -> Region | None:
        gid = rid.split(".")[0]
        fid = rid.split(".")[1]
        geog = self.get_geog(gid)
        return geog.get_region(fid)

    def get_sources(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Source]:
        return self._get_objs(Source, *where_clause)

    def get_questions(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Question]:
        return self._get_objs(Question, *where_clause)

    def get_geographies(self, *args, **kwargs):
        """Alias for get_geogs"""
        return self.get_geog(*args, **kwargs)

    def get_geogs(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Geography]:
        return self._get_objs(Geography, *where_clause)

    def answer_question(
        self,
        question: str | Question,
        region: str | Region,
        time_axis: TimeAxis = None,
    ) -> list[QuestionResultsRow]:
        _question: Question = (
            question if isinstance(question, Question) else self.get_question(question)
        )
        _region: Region = (
            region if isinstance(region, Region) else self.get_region(region)
        )

        if not time_axis:
            time_axis = TimeAxis(
                question.get_temporal_resolution(_region.geog_level),
                "current",
            )

        return self._answer_question(_question, _region, time_axis)

    def _get_obj(self, model: Type[T], oid: str) -> T | None:
        try:
            with Session(self.engine) as session:
                return session.scalars(select(model).where(model.id.like(oid))).first()
        except Exception as e:
            print(e)
            return None

    def _get_objs(
        self,
        model: Type[T],
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[T] | None:
        try:
            with Session(self.engine) as session:
                return session.scalars(select(model).where(*where_clause)).all()
        except Exception as e:
            print(e)
            return None

    def _answer_question(
        self,
        question: Question,
        region: Region,
        time_axis: TimeAxis,
    ) -> list[QuestionResultsRow]:
        """Finds result for question at geog across points across time axis."""

        # find subgeog that works for this question, could be region if it directly answers it
        subgeog = region.geog_level.get_subgeography_for_question(question)

        # get set of regions of with subgeog type that fit within region
        region_list = list(region.get_subregions(subgeog))
        no_spatial_agg = bool(len(region_list) == 1)

        # get query to get question table at this level
        geog = region_list[0].geog_level
        question_source = question.get_source(geog)
        source_query = question_source.source_query

        #  truncate time to spatial resolution to ensure clean aggregation
        temporal_resolution = question_source.source.temporal_resolution

        # determine fields to request based on datatype
        if no_spatial_agg:
            fields = "MIN(value) as value"  # will only have one result so same as value
        else:
            fields = get_aggregate_fields(question.datatype)

        # query the datastore for the question, aggregating data from smaller subregions if necessary
        # returns a set of records representing the answers for the question for the region across time
        # with a granularity specified in the questions `temporal resolution`

        region_ids = [r.feature_id for r in region_list]
        group_by = "time"
        time_selection = "time"

        time_filter = (
            f""" AND "time" {time_axis.domain_filter}"""
            if time_axis.domain_filter
            else ""
        )

        # roll up time
        #   if possible (not aggregating spatially)
        #   and requested (based on time axis resolution vs question's temporal resolution)
        if no_spatial_agg and temporal_resolution != time_axis.resolution:
            group_by += ", region"
            time_selection = "MIN(time) as start_time, MAX(time) as end_time"

        values: list[QuestionResultsRow] = db.query(
            f"""
          SELECT {time_selection}, {fields}
          FROM (SELECT date_trunc('{temporal_resolution}', "time") as "time", 
                       "value"
                 FROM ({source_query}) as base      
                 WHERE "region" = ANY(%s) {time_filter}) as filtered
          GROUP BY {group_by}
          """,
            (region_ids,),
        )

        return values

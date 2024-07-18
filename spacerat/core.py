import os
from os import PathLike
from pathlib import Path
from typing import TypeVar, Type, Sequence

from sqlalchemy import Engine, create_engine, select, ColumnExpressionArgument
from sqlalchemy.orm import Session

from spacerat import db
from spacerat.config import init_db
from spacerat.helpers import get_variant_clause
from spacerat.model import (
    Question,
    TimeAxis,
    Region,
    Geography,
    Source,
    RegionSet,
    QuestionSet,
)
from spacerat.types import QuestionResultsRow

DEFAULT_ENGINE = create_engine("sqlite://")
DEFAULT_MODEL_DIR = Path(os.getcwd()) / "model"

TIME_FIELD = "time"

T = TypeVar("T")


QuestionParam = str | Question | Sequence[str] | Sequence[Question] | QuestionSet


RegionParam = str | Region | Sequence[Region] | Sequence[str] | RegionSet | Geography


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

        :param engine: SQLAlchemy engine to manage the SpaceRAT model.
                         Defaults to in-memory sqlite engine.

        :param model_dir: Directory from which to load model configuration files.
        """
        _engine = engine or DEFAULT_ENGINE
        if debug:
            _engine.echo = True
        self.engine = init_db(_engine, model_dir)

    def get_source(self, sid: str | Source) -> Source | None:
        """Returns the Source object with id `sid`"""
        if isinstance(sid, Source):
            return sid
        return self._get_obj(Source, sid)

    def get_question(self, qid: str | Question) -> Question | None:
        """Returns the Question object with id `qid`"""
        if isinstance(qid, Question):
            return qid
        return self._get_obj(Question, qid)

    def get_geography(self, gid: str | Geography) -> Geography | None:
        """Alias for get_geog"""
        if isinstance(gid, Geography):
            return gid
        return self.get_geog(gid)

    def get_geog(self, gid: str | Geography) -> Geography | None:
        """Returns the Geography object with id `gid`"""
        if isinstance(gid, Geography):
            return gid
        return self._get_obj(Geography, gid)

    def get_region(self, rid: str | Region) -> Region | None:
        """Returns the Region object with id `rid`"""
        if isinstance(rid, Region):
            return rid

        gid = rid.split(".")[0]
        fid = rid.split(".")[1]
        geog = self.get_geog(gid)
        return geog.get_region(fid)

    def get_sources(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Source]:
        """Returns set of Sources filtered by where clause"""
        return self._get_objs(Source, *where_clause)

    def get_questions(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Question]:
        """Returns set of Questions filtered by where clause"""
        return self._get_objs(Question, *where_clause)

    def get_geographies(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Geography]:
        """Alias for get_geogs"""
        return self.get_geogs(*where_clause)

    def get_geogs(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[Geography]:
        """Returns set of Geographies filtered by where clause"""
        return self._get_objs(Geography, *where_clause)

    def _parse_region_arg(self, args: "RegionParam") -> "RegionSet":
        """Generate RegionSet by funneling all possible argument formats."""

        if isinstance(args, RegionSet):
            return args

        if isinstance(args, Region):
            return RegionSet(args)

        if isinstance(args, Geography):
            return RegionSet("ALL", geog_level=args)

        if isinstance(args, str):  # can be geog or region
            if "." in args:
                return RegionSet(self.get_region(args))
            else:
                geog_level = self.get_geography(args)
                return RegionSet("ALL", geog_level=geog_level)

        if isinstance(args, Sequence):
            return RegionSet(*args)

    def _parse_question_arg(
        self,
        args: "QuestionParam",
        geog: "Geography",
    ) -> "QuestionSet":
        """Generate QuestionSet by funneling all possible argument formats."""

        def _parse(_args: str | Question) -> "Question":
            return self.get_question(_args)

        # if a questionset is passed, check it
        if isinstance(args, QuestionSet):
            if not args.directly_describes(geog):
                # todo: try to rebuild the questionset with a smaller geog
                raise ValueError("Questions don't describe this Geography.")
            return args

        # first, turn args into a list of questions
        questions: list["Question"]
        if isinstance(args, Sequence) and not isinstance(args, str):
            questions = [_parse(q) for q in args]
        else:
            questions = [_parse(args)]

        # test for shared source
        first_source, subgeog = questions[0].get_source_and_subgeog(geog)
        if not first_source:
            raise ValueError("No Source found for this Question at this Geography.")

        for question in questions[1:]:
            test_source = question.get_question_source_for_geog(geog)
            if not test_source:
                raise ValueError("No source found for this Question at this Geography.")
            if test_source.source != first_source:
                raise ValueError(
                    "The Questions provided don't use the same Source for this geography."
                )

        # build a questionset and return it
        return QuestionSet(first_source, *questions)

    def answer_question(
        self,
        question: QuestionParam,
        region: RegionParam,
        time_axis: TimeAxis = None,
        variant: str = None,
    ) -> list[QuestionResultsRow]:
        """
        Finds the descriptive statistics of `question` for `region` across `time_axis`.

        Can answer all questions in a QuestionSet across a RegionSet across `time_axis`.


        :param question: The Question to find statistics for. Can directly provide object or reference by ID.

        :param region: The Region or set of Regions to find statistics of `question` for. Can directly provide object
                         or reference by ID.

        :param time_axis: (optional) Time axis across which stats will be calculated. Defaults to the most recent period
                            of time that matches the question's source's temporal resolution.

        :param variant: (optional) Geographic variant to use when calculating the statistics. Filters set of
                          subgeographies considered in calculations.

        :return: A list results by time periods from the time axis. Statistics available in results depends on
                    underlying data type.
        """
        # Parse and standardize arguments
        region_set: "RegionSet" = self._parse_region_arg(region)
        question_set = self._parse_question_arg(
            question,
            region_set.geog_level,
        )

        # default time axis is current
        if not time_axis:
            time_axis = TimeAxis(
                question_set.source.temporal_resolution, domain="current"
            )

        # query for results
        return self._answer(question_set, region_set, time_axis, variant)

    @staticmethod
    def _answer(
        questions: QuestionSet,
        regions: RegionSet,
        time_axis: TimeAxis,
        variant: str = None,
    ) -> list[QuestionResultsRow]:
        """Finds result for questions across multiple regions across points across time axis."""

        source: Source = questions.source
        temporal_resolution = source.temporal_resolution

        # find subgeog that works for this question, could be region if it directly answers it
        geog: Geography = regions.geog_level
        subgeog: Geography = regions.geog_level.get_subgeography_for_question(questions)

        # generate  query that results in region, parent table
        variant_clause = get_variant_clause(subgeog, variant)
        subregions_query = f"""
            SELECT subregion."{subgeog.id_field}" as subregion_id,
                   parent."{geog.id_field}" as parent_id
                   
            FROM "{subgeog.table}" subregion 
              JOIN "{geog.table}" parent 
                ON ST_Covers(parent.geom, subregion.centroid)
                
            {'WHERE parent."{geog.id_field}" IN ({regions.sql_list})' if regions.feature_ids != 'ALL' else ""} {variant_clause}
            """.strip()

        # there's no spatial aggregation when the geog has not been subdivided
        spatial_agg: bool = geog.id != subgeog.id

        # get query to get raw data table
        raw_select_chunks = [
            q.get_question_source_for_geog(subgeog).value_clause for q in questions
        ]  # [ '"source_field_name" as "question_field_name"', ... ]

        source_query = f"""
          SELECT ({source.region_select})  as "region",
                 ({source.time_select})    as "time",
                 {", ".join(raw_select_chunks)}
          FROM {source.table}
        """.strip()

        # determine aggregate fields to use based on datatype these are part top-most select clause
        # that aggregates the data in the raw source query
        if spatial_agg:
            agg_select_chunks = [q.aggregate_select_chunk for q in questions]
        else:
            agg_select_chunks = [
                f"MIN({q.field_name}) as {q.field_name}" for q in questions
            ]

        # query the datastore for the question, aggregating data from smaller subregions if necessary
        # returns a set of records representing the answers for the question for the region across time
        # with a granularity specified in the questions `temporal resolution`
        group_by = "time, parent_id"
        time_selection = f'"{TIME_FIELD}"'
        time_filter = (
            f"""WHERE "{TIME_FIELD}" {time_axis.domain_filter}"""
            if time_axis.domain_filter
            else ""
        )

        # handle aggregation across time
        if not spatial_agg and temporal_resolution != time_axis.resolution:
            group_by += ", parent_region"
            time_selection = (
                f'MIN("{TIME_FIELD}") as start_time, MAX("{TIME_FIELD}") as end_time'
            )

        # todo: replace all with SQLAlchemy once we know what we're doin'
        query = f"""
          SELECT 
            {time_selection},
            {', '.join(agg_select_chunks)}, 
            '{geog.id}.' || "parent_id" as region
          FROM (SELECT date_trunc('{temporal_resolution}', "time") as "{TIME_FIELD}",  
                       {", ".join([q.field_name for q in questions])}, 
                       "region", 
                       regions."parent_id" as "parent_id"
                 FROM ({source_query}) as data 
                   JOIN ({subregions_query}) as regions
                     ON data.region = regions.subregion_id                
                 {time_filter}) as filtered
                 
          GROUP BY {group_by}
          """

        print("\n\n\n", query, "\n\n\n")

        values: list[QuestionResultsRow] = db.query(query)

        return values

    def _get_obj(self, model: Type[T], oid: str) -> T | None:
        try:
            with Session(self.engine) as session:
                return session.scalars(select(model).where(model.id.like(oid))).first()
        except Exception as e:
            raise e

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

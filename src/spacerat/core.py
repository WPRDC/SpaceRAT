from __future__ import annotations

import datetime
import os
import re
from os import PathLike
from pathlib import Path
from typing import TypeVar, Type, Sequence, Mapping, Any, Iterable, Literal
from urllib.parse import urlparse

import jenkspy
import psycopg2
import psycopg2.extras
from ckanapi import RemoteCKAN
from psycopg2.sql import Composable
from slugify import slugify
from sqlalchemy import Engine, create_engine, select, ColumnExpressionArgument
from sqlalchemy.orm import Session

from spacerat.config import init_db
from spacerat.helpers import get_subgeog_clause, parse_period_name
from spacerat.models import (
    Question,
    TimeAxis,
    Region,
    Geography,
    Source,
    RegionSet,
    QuestionSet,
    MapConfig,
)
from .types import AggregateResultsRow

TIME_FIELD = "time"

T = TypeVar("T")

QuestionParam = str | Question | Sequence[str] | Sequence[Question] | QuestionSet

RegionParam = str | Region | Sequence[Region] | Sequence[str] | RegionSet | Geography

GeomOption = Literal["raw"] | Literal["geojson"] | None

SPACERAT_DB_URL = os.environ.get("SPACERAT_DB_URL", "sqlite://")
SPACERAT_DATASTORE_READ_URL = os.environ.get("SPACERAT_DATASTORE_URL")
SPACERAT_DATASTORE_WRITE_URL = os.environ.get("SPACERAT_DATASTORE_WRITE_URL")
SPACERAT_SCHEMA = os.environ.get("SPACERAT_SCHEMA", "spacerat")
SPACERAT_MODEL_DIR = os.environ.get("SPACERAT_MODEL_DIR", Path(os.getcwd()) / "model")

ckan_url = os.environ.get("CKAN_URL", "https://data.wprdc.org")

ckan = RemoteCKAN(ckan_url)


class SpaceRAT:
    engine: Engine

    def __init__(
        self,
        db_url: str = SPACERAT_DB_URL,
        source_read_url: str = SPACERAT_DATASTORE_READ_URL,
        source_write_url: str = SPACERAT_DATASTORE_WRITE_URL,
        schema: str = SPACERAT_SCHEMA,
        model_dir: PathLike = SPACERAT_MODEL_DIR,
        debug: bool = False,
        skip_init=True,
        skip_maps=False,
    ) -> None:
        """
        Initialize a SpaceRAT instance.

        :param db_url: DB connection URL for spacerat ontology storage
        :param source_read_url: URL used to read source data
        :param source_write_url: (optional) URL used to write to source database.
        :param schema: (optional) custom schema name for spacerat objects in source db. default='spacerat'
        :param model_dir: Directory from which to load model configuration files.
        :param debug : Enable debug mode.
        """
        _engine = create_engine(db_url)

        self.source_read_url = source_read_url
        self.source_write_url = source_write_url
        self.schema = schema
        self.model_dir = model_dir

        if skip_init:
            self.engine = _engine
        else:
            self.engine = init_db(_engine, model_dir, skip_maps=skip_maps)

    def reinit(self, skip_maps: bool = False) -> None:
        self.engine = init_db(
            self.engine, model_dir=self.model_dir, skip_maps=skip_maps
        )

    @property
    def db_url(self) -> str:
        return str(self.engine.url)

    def get_map_config(self, mid: str | MapConfig) -> MapConfig:
        """Returns the MapConfig object with id `mid`"""
        if isinstance(mid, MapConfig):
            return mid
        return self._get_obj(MapConfig, mid)

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

    def get_map_configs(
        self,
        *where_clause: ColumnExpressionArgument,
    ) -> Sequence[MapConfig]:
        """Returns set of Maps filtered by where clause"""
        return self._get_objs(MapConfig, *where_clause)

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

    def has_question(self, qid: str) -> bool:
        try:
            q = self.get_question(qid)
            return q is not None
        except:
            return False

    def create_geog_index(self, geog_level: str, replace=True):
        """

        :param geog_level:
        :param replace:
        :return:
        """
        geog = self.get_geog(geog_level)
        table = f'"{self.schema}"."{geog.table}"'
        user = urlparse(self.source_read_url).username

        # ensure schema exists
        self._write_to_db(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

        # (re)create materialized view
        if replace:
            self._write_to_db(f"DROP MATERIALIZED VIEW IF EXISTS {table} CASCADE")
        self._write_to_db(f"""CREATE MATERIALIZED VIEW {table} AS {geog.query}""")

        # set permissions
        self._write_to_db(f"GRANT SELECT ON {table} TO {user}")

        # create common indexes on columns
        self._write_to_db(
            f"CREATE INDEX {geog.table}__id__idx " f"ON {table} ({geog.id_field})"
        )
        self._write_to_db(
            f"CREATE INDEX {geog.table}__name_idx "
            f"ON {table} USING GIN (name gin_trgm_ops)"
        )
        self._write_to_db(
            f"CREATE INDEX {geog.table}__geom__idx " f"ON {table} USING GIST (geom)"
        )
        self._write_to_db(
            f"CREATE INDEX {geog.table}__centroid__idx "
            f"ON {table} USING GIST (centroid)"
        )

        # create any extra indexes
        for index in geog.trigram_indexes:
            self._write_to_db(
                f"CREATE INDEX {geog.table}__{slugify(index, separator='_')}__trgm_idx "
                f"ON {table} USING GIN ({index} gin_trgm_ops)"
            )

    def create_geog_association_tables(self):
        """Creates materialized views that relate a geography to its subgeographies."""
        for geog in self.get_geogs():
            # create subgeog mapping table
            for subgeog in geog.subgeographies:
                table_name = slugify(f"{geog.id}_to_{subgeog.id}", separator="_")
                geog_field = slugify(geog.id, separator="_")
                subgeog_field = slugify(subgeog.id, separator="_")
                print(table_name)
                self._write_to_db(
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.schema}.{table_name} AS "
                    f"SELECT geog.{geog.id_field} as {geog_field}, subgeog.{subgeog.id_field} as {subgeog_field} "
                    f"FROM {self.schema}.{geog.table} geog JOIN {self.schema}.{subgeog.table} subgeog "
                    f"ON ST_Covers(geog.geom, subgeog.centroid) "
                )
                self._write_to_db(
                    f"CREATE INDEX IF NOT EXISTS {table_name}__geog__idx ON {self.schema}.{table_name} ({geog_field})"
                )

                self._write_to_db(
                    f"CREATE INDEX IF NOT EXISTS {table_name}__subgeog__idx ON {self.schema}.{table_name} ({subgeog_field})"
                )

    def update_maps(self, map_id: str, replace: bool = True):
        """Creates a materialized view in the source database using a MapConfig definition."""

        map_config = self.get_map_config(map_id)

        base_questions = [q.id for q in map_config.questions]

        for geog in map_config.geographies:
            # make standard map
            name = map_config.get_view_name(geog.id)
            self.create_map_table(
                geog.id,
                map_config.source_id,
                included_questions=base_questions,
                replace=replace,
                view_name=name,
            )

            # make map for each variant
            for map_variant in map_config.variants:
                name = map_config.get_view_name(
                    geog.id, variant_id=map_variant.variant_id
                )
                variant_questions = [q.id for q in map_variant.questions]
                self.create_map_table(
                    geog.id,
                    map_config.source_id,
                    variant=map_variant.variant_id,
                    included_questions=base_questions + variant_questions,
                    replace=replace,
                    view_name=name,
                )

    def create_map_table(
        self,
        geog_level: str,
        source_id: str,
        included_questions: Iterable[str] = None,
        excluded_questions: Iterable[str] = None,
        variant: str = None,
        filter: str = None,
        filter_arg: str = None,
        replace: bool = True,
        view_name: str = None,
    ):
        """
        Creates a table in the source database with data from a `Source` for all regions of a `Geography`

        :param geog_level: ID of `Geography` to make map for.
        :param source_id: ID of `Source` to pull data from.
        :param included_questions: (optional) IDs of questions to include in the map.
                                If not provided, will all possible questions will be used.
        :param excluded_questions: (optional) IDs of questions to exclude from the map.
                                Will override any values in `included_questions`
        :param replace:  If `True`, will replace existing table with new data.
        :return:
        """
        geog = self.get_geog(geog_level)
        source = self.get_source(source_id)

        # get set of questions to use
        questions = source.questions

        if included_questions:
            questions = [q for q in questions if q.id in included_questions]
        if excluded_questions:
            questions = [q for q in questions if not q.id in excluded_questions]

        # get sql from spacerat query for those questions across all regions in geog
        query, _ = self.get_queries(
            questions,
            geog,
            variant=variant,
            filter=filter,
            filter_arg=filter_arg,
            geom="raw",
        )

        # create materialized view from the query
        view_name = view_name or slugify(
            f"{source_id}__{geog_level}__{variant or ''}", separator="_"
        )
        full_view_name = f'"{self.schema}"."{view_name}"'

        # clear first if allowed
        if replace:
            self._write_to_db(f"""DROP MATERIALIZED VIEW IF EXISTS {full_view_name}""")

        # create materialized view and add indexes to it
        self._write_to_db(f"CREATE MATERIALIZED VIEW {full_view_name} AS {query}")
        self._write_to_db(
            f"CREATE INDEX IF NOT EXISTS {view_name}__geom__idx ON {full_view_name} USING GIST (geom)"
        )
        self._write_to_db(
            f"CREATE INDEX IF NOT EXISTS {view_name}__centroid__idx "
            f"ON {full_view_name} USING GIST (centroid)"
        )
        self._write_to_db(
            f"CREATE INDEX IF NOT EXISTS {view_name}__region_id__idx "
            f"ON {full_view_name} (region_id)"
        )

    def calculate_breaks(
        self,
        mapset_id: str,
        geog_level: str,
        question_id: str,
        stat: str,
        variant: str = None,
        n_classes=5,
    ) -> list[float] | None:
        n_classes = 5 if n_classes is None else n_classes

        # use args to find map table
        mapset = self.get_map_config(mapset_id)
        question = self.get_question(question_id)
        table = mapset.get_view_name(geog_level, variant_id=variant)
        field = f"{question.field_name}__{stat}"

        # calculate the breaks from the map data
        qry = f"""SELECT "{field}" as value FROM "{self.schema}"."{table}" """
        data: list[float] = [row["value"] for row in self._query_db(qry)]
        jnb = jenkspy.JenksNaturalBreaks(n_classes)
        jnb.fit(data)
        return jnb.inner_breaks_

    def get_queries(
        self,
        question: QuestionParam,
        region: RegionParam,
        time_axis: TimeAxis = None,
        variant: str = None,
        filter: str = None,
        filter_arg: str = None,
        geom: GeomOption = None,
    ) -> tuple[str, str]:
        """
        Dry run of `answer_question`.

        :param question:
        :param region:
        :param time_axis:
        :param variant:
        :param filter:
        :param filter_arg:
        :return: full_query, inside_query
        """
        # Parse and standardize arguments
        region_set, question_set, time_axis = self._parse_args(
            question, region, time_axis
        )

        if question_set.source.spatial_resolution == "point":
            return self._get_direct_query(
                questions=question_set,
                regions=region_set,
                time_axis=time_axis,
                geom=geom,
            )

        return self._get_query(
            questions=question_set,
            regions=region_set,
            time_axis=time_axis,
            variant=variant,
            filter=filter,
            filter_arg=filter_arg,
            geom=geom,
        )

    def answer_question(
        self,
        question: QuestionParam,
        region: RegionParam,
        time_axis: TimeAxis = None,
        variant: str = None,
        filter: str = None,
        filter_arg: str = None,
        aggregate: bool = True,
        query_records: bool = False,
        geom: GeomOption = None,
        # add option that allows the set of regions provided to be spatially unioned and treated as one big single geog (e.g. hill district)
    ) -> tuple[list[AggregateResultsRow], list]:
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

        :param aggregate: (optional) If True, calculate and return aggregate statistics at the level of the `region`
            argument's `geography`.

        :param filter: (optional) Filter subgeog based on an argument

        :param filter_arg: (optional) Argument used in filter

        :param query_records: If True, query and return individual records at subgeog level.

        :return: A list results by time periods from the time axis. Statistics available in results depends on
            underlying data type.
        """
        # Parse and standardize arguments

        region_set, question_set, time_axis = self._parse_args(
            question, region, time_axis
        )

        if not aggregate and not query_records:
            print(
                "Neither `aggregate` nor `subgeog_records` is false, returning nothing."
            )
            return [], []

        # query for results
        return self._answer(
            question_set,
            region_set,
            time_axis,
            variant=variant,
            filter=filter,
            filter_arg=filter_arg,
            aggregate=aggregate,
            query_records=query_records,
            geom=geom,
        )

    def _check_cache(self, view_name: str) -> bool:
        results = self._query_db(
            f"SELECT EXISTS(SELECT FROM pg_matviews WHERE schemaname LIKE '{self.schema}' AND matviewname LIKE '{view_name}');"
        )
        return results[0]["exists"]

    def _get_direct_query(
        self,
        questions: QuestionSet,
        regions: RegionSet,
        time_axis: TimeAxis,
        geom: GeomOption = None,
    ):
        source: Source = questions.source
        temporal_resolution = source.temporal_resolution
        geog: Geography = regions.geog_level

        # standardize select statements
        raw_select_chunks = [
            q.value_clause for q in questions
        ]  # [ '"source_field_name" as "question_field_name"', ... ]

        source_query = f"""
              SELECT _geom                     as "geom",
                     ({source.time_select})    as "time",
                     {", ".join(raw_select_chunks)}
              FROM "{source.table}"
            """.strip()

        agg_select_chunks = [q.aggregate_select_chunk for q in questions]

        # handle filtering data
        time_filter_clause = (
            f""" "{TIME_FIELD}" {time_axis.domain_filter} """
            if time_axis.domain_filter
            else ""
        )

        has_filters = bool(time_filter_clause)

        inner_where_clause = f"WHERE {time_filter_clause} " if has_filters else ""

        # todo: limit to time axis

        inside_query = f"""
                SELECT date_trunc('{temporal_resolution}', "time") as "{TIME_FIELD}",  
                               {", ".join([q.field_name for q in questions])}, 
                               geog.geom,
                               geog.id                             as "region_id"

                FROM ({source_query}) as raw_data 
                  JOIN "{self.schema}"."{geog.table}" as geog
                    ON ST_Covers(geog.geom, raw_data.geom)
                {inner_where_clause}
            """.strip()

        geo_select, geo_join = self._get_geog_select_and_join(geog, geom)

        query = f"""
             SELECT data.* {geo_select}
             FROM (SELECT 
                     "{TIME_FIELD}",
                     {', '.join(agg_select_chunks)}, 
                     '{geog.id}.' || "region_id"     as region,
                     region_id
                   FROM ({inside_query}) time_filtered 
                   GROUP BY region_id, time) data
               {geo_join}
             """

        return query, inside_query

    def _get_query(
        self,
        questions: QuestionSet,
        regions: RegionSet,
        time_axis: TimeAxis,
        variant: str = None,
        filter: str = None,
        filter_arg: str = None,
        geom: GeomOption = None,
    ) -> (str, str):
        """

        :param questions:
        :param regions:
        :param time_axis:
        :param variant:
        :param filter:
        :param filter_arg:
        :return: (full_query, inside_query)
        """
        source: Source = questions.source
        temporal_resolution = source.temporal_resolution

        # find subgeog that works for this question, could be region if it directly answers it
        geog: Geography = regions.geog_level

        subgeog: Geography = regions.geog_level.get_subgeography_for_question(questions)

        geog_field = slugify(geog.id, separator="_")
        subgeog_field = slugify(subgeog.id, separator="_")
        geo_mapping_table = f"{geog_field}_to_{subgeog_field}"

        # there's no spatial aggregation when the geog has not been subdivided
        spatial_agg: bool = geog.id != subgeog.id

        # get query to get raw data table
        raw_select_chunks = [
            q.value_clause for q in questions
        ]  # [ '"source_field_name" as "question_field_name"', ... ]

        source_query = f"""
              SELECT ({source.region_select})  as "region",
                     ({source.time_select})    as "time",
                     {", ".join(raw_select_chunks)}
              FROM "{source.table}"
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
        group_by = "time, region_id"
        time_selection = f'"{TIME_FIELD}"'
        # handle aggregation across time
        if not spatial_agg and temporal_resolution != time_axis.resolution:
            group_by += ", parent_region"
            time_selection = (
                f'MIN("{TIME_FIELD}") as start_time, MAX("{TIME_FIELD}") as end_time'
            )

        # handle filtering data
        time_filter_clause = (
            f""" "{TIME_FIELD}" {time_axis.domain_filter} """
            if time_axis.domain_filter
            else ""
        )
        variant_clause = get_subgeog_clause(subgeog.variants, variant)
        filter_clause = get_subgeog_clause(subgeog.filters, filter)

        # todo: add more sql that filters regions by spatial domain - may require another table
        # spatial_domain_clause

        filters = " AND ".join(
            clause
            for clause in [time_filter_clause, variant_clause, filter_clause]
            if clause
        )
        has_filters = bool(variant_clause or filter_clause or time_filter_clause)

        inner_where_clause = f"WHERE {filters} " if has_filters else ""

        # pull extra fields from subgeog (e.g. address of parcel)
        extra_fields_select_clause: str = ""
        if subgeog.extra_fields:
            extra_fields_select_clause += ", "
            extra_fields_select_clause += ", ".join(
                [f"subgeogs.{extra_field}" for extra_field in subgeog.extra_fields]
            )

        # todo: replace all with SQLAlchemy once we know what we're doin'
        # generate query that results in region, parent table and is limited to the source's spatial domain
        inside_query = f"""
                SELECT date_trunc('{temporal_resolution}', "time") as "{TIME_FIELD}",  
                               {", ".join([q.field_name for q in questions])}, 
                               "region", 
                               regions."{geog_field}"          as "region_id"
                               {extra_fields_select_clause}
                               
                FROM ({source_query}) as raw_data 
                  JOIN "{self.schema}"."{geo_mapping_table}" as regions ON raw_data.region = regions."{subgeog_field}"
                  JOIN "{self.schema}"."{subgeog.table}" as subgeogs ON regions."{subgeog_field}" = subgeogs."{subgeog.id_field}"
                {inner_where_clause} 
            """.strip()

        geo_select, geo_join = self._get_geog_select_and_join(geog, geom)

        query = f"""
        SELECT data.* {geo_select}
        FROM (SELECT 
                {time_selection},
                {', '.join(agg_select_chunks)}, 
                '{geog.id}.' || "region_id"     as region,
                region_id
              FROM ({inside_query}) time_filtered 
              GROUP BY {group_by}) data
          {geo_join}
        """

        # generate queries with params
        params = [filter_arg] if filter_arg else []

        inside_query = self._mogrify_query(inside_query, params)
        full_query = self._mogrify_query(query, params)

        return full_query, inside_query

    def _answer(
        self,
        questions: QuestionSet,
        regions: RegionSet,
        time_axis: TimeAxis,
        variant: str = None,
        filter: str = None,
        filter_arg: str = None,
        aggregate: bool = True,
        query_records: bool = False,
        geom: GeomOption = None,
    ) -> tuple[list[AggregateResultsRow], list]:
        """Finds result for questions across multiple regions across points in time axis."""

        if questions.source.spatial_resolution == "point":
            query, inside_query = self._get_direct_query(
                questions=questions,
                regions=regions,
                time_axis=time_axis,
                geom=geom,
            )
        else:
            query, inside_query = self._get_query(
                questions=questions,
                regions=regions,
                time_axis=time_axis,
                variant=variant,
                filter=filter,
                filter_arg=filter_arg,
                geom=geom,
            )

        # query and return requested data
        records = []
        values = []
        if query_records:
            records = self._query_db(inside_query)
        if aggregate:
            values: list[AggregateResultsRow] = self._query_db(query)

        return values, records

    def _parse_args(
        self,
        question: QuestionParam,
        region: RegionParam,
        time_axis: TimeAxis = None,
    ):
        region_set: "RegionSet" = self._parse_region_arg(region)
        question_set = self._parse_question_arg(
            question,
            region_set.geog_level,
        )

        # default time axis is current, or most current unit available in source
        if not time_axis:
            source = question_set.source
            resolution = source.temporal_resolution
            if question_set.source.archived:
                end = datetime.datetime.fromisoformat(
                    ckan.action.datastore_search_sql(
                        sql=f"""SELECT {source.time_select} as time FROM "{source.table}" ORDER BY time DESC LIMIT 1"""
                    )["records"][0]["time"]
                )
                start = end - parse_period_name(source.temporal_resolution)
                time_axis = TimeAxis(
                    resolution,
                    domain=(start, end),
                )
            else:
                time_axis = TimeAxis(
                    question_set.source.temporal_resolution, domain="current"
                )

        return region_set, question_set, time_axis

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
                print(args)
                geog_level = self.get_geography(args)
                return RegionSet("ALL", geog_level=geog_level)

        if isinstance(args, Sequence):
            if type(args[0]) == str:
                return RegionSet(*[self.get_region(arg) for arg in args])

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
            test_source = question.source
            if not test_source:
                raise ValueError("No source found for this Question at this Geography.")
            if test_source != first_source:
                raise ValueError(
                    "The Questions provided don't use the same Source for this geography."
                )

        # build a questionset and return it
        return QuestionSet(first_source, *questions)

    def _get_geog_select_and_join(
        self, geog, geom: GeomOption = None
    ) -> tuple[str, str]:
        """Get select statement and join statement used to link final aggregate data with geometries"""
        geo_select = ""
        if geom == "raw":
            geo_select = ", geo.geom, geo.centroid"
        if geom == "geojson":
            geo_select = ", ST_AsGeoJSON(geo.geom) as geom, ST_AsGeoJSON(geo.centroid) as centroid"

        geo_join = ""
        if geom:
            geo_join = f'JOIN "{self.schema}"."{geog.table}" geo ON geo."{geog.id_field}" = data.region_id'
        return geo_select, geo_join

    def _get_obj(self, model: Type[T], oid: str) -> T | None:
        try:
            with Session(self.engine) as session:
                result = session.scalars(
                    select(model).where(model.id.like(oid))
                ).first()
                session.expunge_all()
                return result
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
                if where_clause:
                    results = (
                        session.scalars(select(model).where(*where_clause))
                        .unique()
                        .all()
                    )
                else:
                    results = session.scalars(select(model)).unique().all()
                session.expunge_all()
                return results
        except Exception as e:
            print(e)
            return None

    def _query_db(
        self,
        q: str | bytes | Composable,
        params: Sequence | Mapping[str, Any] | None = None,
    ) -> list[dict]:
        with psycopg2.connect(self.source_read_url) as conn:
            qry = str(re.sub(r"\s+", " ", q).strip())
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(qry, params)
                results = cur.fetchall()
        conn.close()
        return results

    def _mogrify_query(
        self,
        q: str | bytes | Composable,
        params: Sequence | Mapping[str, Any] | None = None,
    ) -> str:
        with psycopg2.connect(self.source_read_url) as conn:
            qry = str(re.sub(r"\s+", " ", q).strip())
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                result = cur.mogrify(qry, params).decode("utf-8")
        conn.close()
        return result

    def _write_to_db(
        self,
        q: str | bytes | Composable,
        params: Sequence | Mapping[str, Any] | None = None,
    ) -> None:
        if not self.source_write_url:
            raise ValueError("source_write_url must be set")

        with psycopg2.connect(self.source_write_url) as conn:
            qry = str(re.sub(r"\s+", " ", q).strip())
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                print("🆒", qry)
                cur.execute(qry, params)
        conn.close()

    def get_sql_spatial_domain(self, source: Source) -> str:
        regions = {}
        # group by geog
        for domain_region in source.spatial_domain:
            g_id, r_id = domain_region.split(".")[0:2]
            if g_id not in regions:
                regions[g_id] = []
            regions[g_id].append(self.get_region(domain_region))

        region_sets = []
        for g_id, regions in regions.items():
            region_sets.append(RegionSet(*regions, geog_level=self.get_geog(g_id)))

        # creates one big chain of UNION statements unifying all the extents
        region_union = "\nUNION\n".join([rs.extent_query for rs in region_sets])

        return f"(SELECT ST_Union(the_geom) FROM ({region_union})) as region_union"

    def get_breaks(
        self,
    ):
        pass

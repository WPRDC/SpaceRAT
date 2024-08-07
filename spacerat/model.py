import datetime
from dataclasses import dataclass
from typing import Optional, Union, Sequence, Literal, Iterator

from sqlalchemy import String, Text, ForeignKey, DateTime, Table, Column
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    attribute_keyed_dict,
)
from typing_extensions import Tuple

from spacerat.helpers import parse_period_name, as_field_name, get_aggregate_fields
from spacerat.types import TemporalResolution, DataType, TemporalDomain

_combine_dt = datetime.datetime.combine

# SQLAlchemy Models


class Base(DeclarativeBase):
    def from_config(self, **kwargs):
        raise NotImplementedError


class Question(Base):
    """
    Represents a question mapping user ontology to source data.

    These often map directly to fields in our datasets.
    """

    __tablename__ = "question"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    datatype: Mapped[DataType] = mapped_column(String(20))
    sources: Mapped[list["QuestionSource"]] = relationship(
        back_populates="question", cascade="all, delete-orphan", lazy="selectin"
    )

    def __repr__(self):
        return (
            f"Question(id={self.id!r}, name={self.name!r}, datatype={self.datatype!r})"
        )

    def __eq__(self, other):
        if isinstance(other, Question):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    @property
    def field_name(self):
        return as_field_name(self.id)

    @property
    def aggregate_select_chunk(self) -> str:
        return get_aggregate_fields(self)

    @property
    def spatial_resolutions(self) -> list[str]:
        """The geographic levels this question directly describes"""
        return [qs.source.spatial_resolution for qs in self.sources]

    def directly_describes(self, geog: Union["Geography", str]) -> bool:
        """Returns True if this question can be answered for the geog_type without aggregation."""
        if type(geog) is str:
            geog_id = geog
        else:
            geog_id = geog.id

        for qs in self.sources:
            if geog_id == qs.source.spatial_resolution:
                return True
        return False

    def get_question_source_for_geog(
        self, geog: "Geography"
    ) -> Optional["QuestionSource"]:
        """Returns QuestionSource at `geog` level if it exists."""
        for qsource in self.sources:
            if qsource.source.spatial_resolution == geog.id:
                return qsource
        return None

    def get_source_and_subgeog(
        self, geog: "Geography"
    ) -> Tuple[Optional["Source"], Optional["Geography"]]:
        subgeog = geog.get_subgeography_for_question(self)
        qsource = self.get_question_source_for_geog(subgeog)
        return qsource.source, subgeog

    def get_temporal_resolution(self, geog: "Geography") -> TemporalResolution:
        qs = self.get_question_source_for_geog(geog)
        if qs:
            return qs.source.temporal_resolution

    @staticmethod
    def from_config(**kwargs):
        spatial_domain = kwargs["spatial_domain"]
        del kwargs["spatial_domain"]
        if type(spatial_domain) is str:
            spatial_domain_str = spatial_domain
        else:
            spatial_domain_str = ",".join(spatial_domain)
        return Source(**kwargs, spatial_domain_str=spatial_domain_str)


class QuestionSource(Base):
    __tablename__ = "question_source"
    question_id: Mapped[str] = mapped_column(
        ForeignKey("question.id"), primary_key=True
    )
    source_id: Mapped[str] = mapped_column(ForeignKey("source.id"), primary_key=True)
    geography_id: Mapped[str] = mapped_column(
        ForeignKey("geography.id"), primary_key=True
    )

    # related models
    question: Mapped["Question"] = relationship(
        back_populates="sources", lazy="immediate"
    )
    source: Mapped["Source"] = relationship(
        back_populates="questions", lazy="immediate"
    )

    # associated data
    value_select: Mapped[str] = mapped_column(Text())
    geography: Mapped["Geography"] = relationship()

    @property
    def value_clause(self) -> str:
        return f"""{self.value_select} as "{self.field_name}" """

    @property
    def field_name(self) -> str:
        return as_field_name(self.question.id)

    @property
    def raw_field(self):
        return self.value_select


class Source(Base):
    """Defines how to pull data at a geographic level"""

    __tablename__ = "source"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    table: Mapped[str] = mapped_column(String(120))

    spatial_resolution: Mapped[str] = mapped_column(String(120))
    spatial_domain_str: Mapped[str] = mapped_column(Text())

    temporal_resolution: Mapped[TemporalResolution] = mapped_column(String(20))
    temporal_domain_name: Mapped[Optional[TemporalDomain]] = mapped_column(
        String(100), nullable=True
    )
    temporal_domain_start: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(), nullable=True
    )
    temporal_domain_end: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(), nullable=True
    )

    region_select: Mapped[str] = mapped_column(Text())
    time_select: Mapped[str] = mapped_column(Text())

    questions: Mapped[list["QuestionSource"]] = relationship(
        back_populates="source", cascade="all, delete-orphan"
    )

    @property
    def spatial_domain(self) -> list[str]:
        return self.spatial_domain_str.split(",")

    def __repr__(self):
        return f"Source(id={self.id!r}, name={self.name!r}, table={self.table!r})"

    @staticmethod
    def from_config(config):
        spatial_domain = config["spatial_domain"]
        del config["spatial_domain"]
        if type(spatial_domain) is str:
            spatial_domain_str = spatial_domain
        else:
            spatial_domain_str = ",".join(spatial_domain)
        return Source(**config, spatial_domain_str=spatial_domain_str)

    def __eq__(self, other):
        if isinstance(other, Source):
            return self.id == other.id
        return False


geography_association = Table(
    "geography_association",
    Base.metadata,
    Column("parent_id", ForeignKey("geography.id"), primary_key=True),
    Column("child_id", ForeignKey("geography.id"), primary_key=True),
)


class GeographyVariant(Base):
    __tablename__ = "geography_variant"

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    geography_id: Mapped[str] = mapped_column(
        ForeignKey("geography.id"), primary_key=True
    )
    where_clause: Mapped[str] = mapped_column(Text())


class Geography(Base):
    """Represents a type of place, or a geographic level.

    e.g. neighborhood, parcel, tract
    """

    __tablename__ = "geography"

    # slug
    id: Mapped[str] = mapped_column(String(120), primary_key=True)

    # human-friendly name
    name: Mapped[str] = mapped_column(String(120))

    # the query used to make this geog's index table
    query: Mapped[str] = mapped_column(String(120), nullable=True)

    # the name of this geog's index table
    table: Mapped[str] = mapped_column(String(120), nullable=True)

    # the field in `table` that holds region IDs for this geography
    id_field: Mapped[str] = mapped_column(String(120), default="id")

    # the geographies that perfectly subdivide this geography
    subgeographies: Mapped[list["Geography"]] = relationship(
        "Geography",
        secondary=geography_association,
        primaryjoin=id == geography_association.c.parent_id,
        secondaryjoin=id == geography_association.c.child_id,
        lazy="immediate",
    )

    variants: Mapped[dict[str, "GeographyVariant"]] = relationship(
        collection_class=attribute_keyed_dict("id"),
        cascade="all, delete-orphan",
        lazy="immediate",
    )

    def __repr__(self):
        return f"Geography(id={self.id!r}, name={self.name!r}, table={self.table!r})"

    def __eq__(self, other):
        return self.id == other.id

    def get_region(self, fid: str) -> "Region":
        return Region(geog_level=self, feature_id=fid)

    def get_subgeography_for_question(
        self, q: Union["Question", "QuestionSet"]
    ) -> Optional["Geography"]:
        """Find subgeography that question can be directly answered at if any"""
        if q.directly_describes(self):
            return self
        for subgeog in self.subgeographies:
            if q.directly_describes(subgeog):
                return subgeog
        return None


# Dataclasses


@dataclass
class TimeAxis:
    """Defines temporal resolution and domain being requested by user."""

    resolution: TemporalResolution
    domain: tuple[Optional[datetime.datetime], Optional[datetime.datetime]]
    domain_name: str

    def __init__(
        self,
        resolution: TemporalResolution,
        domain: TemporalDomain
        | tuple[datetime.datetime, datetime.datetime]
        | tuple[str, str],
    ):
        self.resolution = resolution
        now = datetime.datetime.now()
        today = datetime.date.today()
        midnight = datetime.time()
        eod = datetime.time(hour=23, minute=59, second=59)

        if isinstance(domain, str):
            self.domain_name = domain
            if domain == "current":
                self.domain = (None, None)

            elif domain.startswith("past-"):
                start = datetime.datetime.now() - parse_period_name(domain[5:])
                self.domain = (start, None)

            elif domain == "last-hour":
                middle = now - datetime.timedelta(hours=1)
                self.domain = (
                    middle.replace(minute=0, second=0, microsecond=0),
                    now.replace(minute=0, second=0, microsecond=0),
                )

            elif domain == "last-day":
                self.domain = (
                    _combine_dt(now.date().replace(day=now.day - 1), midnight),
                    _combine_dt(now.date(), midnight),
                )

            elif domain == "last-week":
                start = now - datetime.timedelta(
                    days=now.weekday(), weeks=1
                )  # last monday
                start = _combine_dt(start.date(), midnight)
                self.domain = (start, start + datetime.timedelta(weeks=1))

            elif domain == "last-month":
                last_month_end = today.replace(day=1) - datetime.timedelta(days=1)
                self.domain = (
                    _combine_dt(last_month_end.replace(day=1), midnight),
                    _combine_dt(last_month_end, eod),
                )

            elif domain == "last-year":
                last_year_end = today.replace(month=1, day=1) - datetime.timedelta(
                    days=1
                )
                self.domain = (
                    _combine_dt(last_year_end.replace(month=1, day=1), midnight),
                    _combine_dt(last_year_end, eod),
                )
        else:
            self.domain_name = "custom"
            start, end = domain
            if isinstance(start, str) and isinstance(end, str):
                self.domain = (
                    datetime.datetime.fromisoformat(start),
                    datetime.datetime.fromisoformat(end),
                )
            else:
                self.domain = (start, end)

    @property
    def iso_domain(self) -> tuple[Optional[str], Optional[str]]:
        return (
            self.domain[0].isoformat() if self.domain[0] else None,
            self.domain[1].isoformat() if self.domain[1] else None,
        )

    @property
    def start(self) -> Optional[datetime.datetime]:
        return self.domain[0]

    @property
    def end(self) -> Optional[datetime.datetime]:
        return self.domain[1]

    @property
    def domain_filter(self) -> str | None:
        if self.start and self.end:
            return f"BETWEEN {self.start.isoformat()} AND {self.end.isoformat()}"

        elif self.start:
            return f"> {self.start.isoformat()}"

        elif self.end:
            return f"< {self.end.isoformat()}"
        else:
            return None


@dataclass
class Region:
    """A specific feature of a geography type.

    e.g. Bloomfield neighborhood, Parcel #0052M00183020000, Tract 42003102000
    """

    geog_level: "Geography"
    feature_id: str

    @property
    def geom_query(self) -> str:
        return (
            f"SELECT geom FROM {self.geog_level.table} WHERE id = '{self.feature_id}'"
        )

    def __hash__(self):
        return hash((self.geog_level, self.feature_id))


# Collections


class QuestionSet:
    """Collection of Questions from the same source."""

    def __init__(self, source: "Source", *questions: "Question"):
        self.source: Source = source
        self.questions: set["Question"] = set([])

        for question in questions:
            self.add_question(question)

    def add_question(self, question: "Question") -> None:
        self._validate_question(question)
        self.questions.add(question)

    def _validate_question(self, question: "Question") -> None:
        if self.source.id not in [qs.source.id for qs in question.sources]:
            raise ValueError(
                "Questions in QuestionSet must all share a common source. "
                f"Test failed for {question}"
            )

    def directly_describes(self, geog: "Geography") -> bool:
        return self.source.spatial_resolution == geog.id

    def get_query_at_geog(self, geog: "Geography") -> str:
        """Returns a query for table of raw data for each question across the regions and time axis."""
        # the raw value select statements chunks for each of the questions in this set
        question_select_chunks = [
            q.get_question_source_for_geog(geog).value_clause for q in self.questions
        ]

        return f"""
          SELECT ({self.source.region_select})  as "region",
                 ({self.source.time_select})    as "time",
                 {", ".join(question_select_chunks)}
          FROM {self.source.table}
        """.strip()

    @staticmethod
    def from_questions(
        questions: Sequence["Question"],
        geog: "Geography",
    ) -> Sequence["QuestionSet"]:
        """Generate QuestionSets from heterogeneous (source-wise) questions.
        One set for each source used at `geog`.
        """
        source_to_questions = {}
        for question in questions:
            question_source = question.get_question_source_for_geog(geog)
            # start queryset first with source
            if question_source.source.id not in source_to_questions:
                source_to_questions[question_source.source.id] = QuestionSet(
                    source=question_source.source
                )
            # add question for QuestionSet with its source at this geog
            source_to_questions[question_source.source.id].add_question(question)
        return list(source_to_questions.values())

    def __add__(self, other: "QuestionSet") -> "QuestionSet":
        if self.source.id != other.source.id:
            raise ValueError(
                "Addition of QuestionSets is only available for those with same source."
            )
        return QuestionSet(
            self.source,
            *self.questions,
            *other.questions,
        )

    def __iter__(self) -> Iterator["Question"]:
        return iter(self.questions)


class RegionSet:
    """Collection of regions of same geographic level."""

    def __init__(
        self,
        region: Literal["ALL"] | "Region",
        *_regions: "Region",
        geog_level: "Geography" = None,
    ) -> None:
        self.geog_level: Geography
        self.feature_ids: set | Literal["ALL"]

        if region == "ALL":
            self.feature_ids = "ALL"
            self.geog_level = geog_level
            if not geog_level:
                raise ValueError(
                    "Special feature_id cases require a geog_level to be specified."
                )
        else:
            self.feature_ids = set()
            for _region in [region, *_regions]:
                if not hasattr(self, "geog_level"):
                    self.geog_level = _region.geog_level
                elif self.geog_level != _region.geog_level:
                    raise ValueError(
                        "Regions in a RegionSet must all be of the same Geography."
                    )
                self.feature_ids.add(_region.feature_id)

    def as_list(self) -> list["Region"]:
        # todo: handle "all"
        return [Region(self.geog_level, fid) for fid in self.feature_ids]

    @property
    def sql_list(self) -> str:
        if self.feature_ids == "ALL":
            return f"SELECT {self.geog_level.id_field} FROM {self.geog_level.table}"
        return ", ".join([f"'{fid}'" for fid in self.feature_ids])

    def at_subgeog(self, subgeog: "Geography") -> "RegionSet":
        """Return a new RegionSet representing regions of smaller geographic level that fit within this region set."""

        subregion_ids: list[str] = []
        # todo: need feature IDs for all the subgregions within this region set.
        #   that means making a query to the subgeog table looking for features that fall within the bounds
        #   of this regionset.
        if self.feature_ids == "ALL":
            return RegionSet("ALL", geog_level=subgeog)

        qry = f"""SELECT {subgeog.id_field} FROM {subgeog.table} as sg
        WHERE sg.
        """

        return RegionSet(
            *[Region(subgeog, fid) for fid in subregion_ids],
            geog_level=subgeog,
        )

    def __add__(self, other: "RegionSet") -> "RegionSet":
        if self.geog_level.id != other.geog_level.id:
            raise ValueError(
                "Addition of RegionSets is only available for those with same geog_level."
            )
        if self.feature_ids == "ALL" and other.feature_ids == "ALL":
            return RegionSet("ALL", geog_level=self.geog_level)

        return RegionSet(*self.feature_ids, *other.feature_ids)

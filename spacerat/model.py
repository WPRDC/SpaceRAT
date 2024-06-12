import datetime
from dataclasses import dataclass
from typing import Optional, Union

from sqlalchemy import String, Text, ForeignKey, DateTime, Table, Column
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    attribute_keyed_dict,
)

from spacerat import db
from spacerat.helpers import parse_period_name
from spacerat.types import TemporalResolution, DataType, TemporalDomain

_combine_dt = datetime.datetime.combine


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

    def get_source(self, geog: "Geography") -> Optional["QuestionSource"]:
        for qs in self.sources:
            if qs.source.spatial_resolution == geog.id:
                return qs
        return None

    def get_temporal_resolution(self, geog: "Geography") -> TemporalResolution:
        qs = self.get_source(geog)
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
    question: Mapped["Question"] = relationship(back_populates="sources")
    source: Mapped["Source"] = relationship(
        back_populates="questions", lazy="immediate"
    )

    # associated data
    value_select: Mapped[str] = mapped_column(Text())
    geography: Mapped["Geography"] = relationship()

    @property
    def source_query(self):
        return f"""
          SELECT ({self.source.region_select})  as "region",
                 ({self.source.time_select})    as "time",
                 ({self.value_select})          as "value"
          FROM {self.source.table}
        """.strip()


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

    # def __init__(self, **kwargs):


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

    def get_region(self, fid: str) -> "Region":
        return Region(geog_level=self, feature_id=fid)

    def get_subgeography_for_question(
        self, question: "Question"
    ) -> Optional["Geography"]:
        """Find subgeography that question can be directly answered at if any"""
        if question.directly_describes(self):
            return self
        for subgeog in self.subgeographies:
            if question.directly_describes(subgeog):
                return subgeog
        return None


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

    def get_subregions(
        self,
        subgeog: "Geography",
        variant: str = None,
    ) -> set["Region"]:
        """Get list of IDs for subregions of this region of a specific geography."""
        if subgeog == self.geog_level:
            return {self}

        # optionally, get extra clause to filter for variant
        variant_clause = None
        if variant is not None and variant in subgeog.variants:
            variant_clause = subgeog.variants[variant].where_clause

        if variant_clause:
            variant_clause = "AND " + variant_clause
        else:
            variant_clause = ""

        # query the data source db
        results: list[[str]] = db.query(
            f"""SELECT {subgeog.id_field} 
                FROM "{subgeog.table}" 
                WHERE ST_Intersects(({self.geom_query}), "{subgeog.table}".geom) 
                    {variant_clause}"""
        )

        return set(subgeog.get_region(row[subgeog.id_field]) for row in results)

    def __hash__(self):
        return hash((self.geog_level, self.feature_id))

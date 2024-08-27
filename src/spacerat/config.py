import uuid
from os import PathLike
from pathlib import Path
from typing import Type

import yaml
from sqlalchemy import select, Engine
from sqlalchemy.orm import Session

from spacerat.models import (
    Base,
    Source,
    Question,
    Geography,
    QuestionSource,
    GeographyVariant,
    GeographyFilter,
    MapConfig,
    MapConfigVariant,
)

_engine: Engine


def _load_source(**kwargs) -> Source:
    spatial_domain = kwargs["spatial_domain"]
    del kwargs["spatial_domain"]
    if type(spatial_domain) is str:
        spatial_domain_str = spatial_domain
    else:
        spatial_domain_str = ",".join(spatial_domain)
    return Source(**kwargs, spatial_domain_str=spatial_domain_str)


def _load_geog(**kwargs) -> Geography:
    del kwargs["subgeographies"]
    variants = kwargs.get("variants", {})
    if "variants" in kwargs:
        del kwargs["variants"]
    filters = kwargs.get("filters", {})
    if "filters" in kwargs:
        del kwargs["filters"]

    geog = Geography(**kwargs, subgeographies=[])

    for variant, clause in variants.items():
        geog.variants[variant] = GeographyVariant(id=variant, where_clause=clause)

    for _filter, clause in filters.items():
        geog.filters[_filter] = GeographyFilter(id=_filter, where_clause=clause)

    return geog


def _load_question(**kwargs) -> Question:
    raw_sources = kwargs["sources"]
    del kwargs["sources"]
    question = Question(**kwargs)
    # connect sources
    with Session(_engine) as session:
        for source_config in raw_sources:
            geog = session.scalars(
                select(Geography).where(Geography.id == source_config["geog"])
            ).first()

            source = session.scalars(
                select(Source).where(Source.id == source_config["source_id"])
            ).first()
            question_source = QuestionSource(
                geography=geog,
                value_select=source_config["value_select"],
            )
            question_source.source = source
            question.sources.append(question_source)
            session.add(question_source)
        session.commit()
    return question


def _load_map(**kwargs) -> MapConfig:
    source_id = kwargs["source"]
    raw_geographies = kwargs["geographies"]
    raw_questions = kwargs["questions"]
    raw_variants = kwargs["variants"]
    del kwargs["source"]
    del kwargs["geographies"]
    del kwargs["questions"]
    del kwargs["variants"]

    with Session(_engine) as session:
        map_config = MapConfig(**kwargs)
        # link source
        source = session.scalars(select(Source).where(Source.id == source_id)).first()
        map_config.source = source
        session.add(map_config)
        # link geographies
        for geog_level in raw_geographies:
            geog = session.scalars(
                select(Geography).where(Geography.id == geog_level)
            ).first()
            map_config.geographies.append(geog)

        # link questions
        for qid in raw_questions:
            question = session.scalars(
                select(Question).where(Question.id == qid)
            ).first()
            map_config.questions.append(question)

        # link variants
        for variant_id, variant_config in raw_variants.items():
            map_variant = MapConfigVariant(
                id=f"{map_config.id}-{variant_id}",
                map_config=map_config,
                variant=variant_id,
            )
            session.add(map_variant)

            if variant_config is not None:
                # link specific questions for variant, if any
                for qid in variant_config.get("questions", []):
                    question = session.scalars(
                        select(Question).where(Question.id == qid)
                    ).first()
                    map_variant.questions.append(question)
        session.commit()
    return map_config


def _init_model(config_dir: PathLike, loader) -> None:
    """Loads all model object files in a directory."""
    config_dir = Path(config_dir)

    files = config_dir.glob("**/*.yaml")

    with Session(_engine) as session:
        objs = []
        for filename in files:
            with open(config_dir / filename) as f:
                config = yaml.safe_load(f)
                objs.append(loader(**config))

        session.add_all(objs)
        session.commit()


def _has_new_files(config_dir: Path, model: Type[Base]) -> bool:
    """Checks for new files in model directories"""
    with Session(_engine) as session:
        objs = session.scalars(select(model)).unique().all()

    return len(list(config_dir.glob("**/*.yaml"))) > len(objs)


def init_db(engine: Engine, model_dir: PathLike, drop=False) -> Engine:
    global _engine
    _engine = engine
    model_dir = Path(model_dir)
    sources_dir = model_dir / "sources"
    geogs_dir = model_dir / "geographies"
    questions_dir = model_dir / "questions"
    maps_dir = model_dir / "maps"

    # (re)Build database
    if drop:
        Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)

    # load Sources
    if _has_new_files(sources_dir, Source):
        print("loading new Sources")
        _init_model(sources_dir, _load_source)

    # load Geographies
    if _has_new_files(geogs_dir, Geography):
        print("loading new Geographies")

        _init_model(geogs_dir, _load_geog)

        # link geogs
        for filename in geogs_dir.glob("*.yaml"):
            with open(geogs_dir / filename) as f:
                config = yaml.safe_load(f)

                with Session(_engine) as session:
                    geog = session.scalars(
                        select(Geography).where(Geography.id == config["id"])
                    ).first()
                    if config["subgeographies"]:
                        for subgeog_id in config["subgeographies"]:
                            subgeog = session.scalars(
                                select(Geography).where(Geography.id == subgeog_id)
                            ).first()

                            geog.subgeographies.append(subgeog)
                        session.commit()

    # load Questions
    if _has_new_files(questions_dir, Question):
        print("loading new Questions")
        _init_model(questions_dir, _load_question)

    # load Maps
    if _has_new_files(maps_dir, MapConfig):
        print("loading new Maps")
        _init_model(maps_dir, _load_map)

    return _engine

from os import PathLike
from pathlib import Path

import yaml
from sqlalchemy import create_engine, select, Engine
from sqlalchemy.orm import Session

from spacerat.model import Base, Source, Question, Geography, QuestionSource

script_dir = Path(__file__).parent.resolve().parent.resolve()
sources_dir = script_dir / "model" / "sources"
geogs_dir = script_dir / "model" / "geographies"
questions_dir = script_dir / "model" / "questions"

_engine = create_engine("sqlite://", echo=True)


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
    return Geography(**kwargs, subgeographies=[])


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
                geography=geog, value_select=source_config["value_select"]
            )
            question_source.source = source
            question.sources.append(question_source)
            session.add(question_source)
        session.commit()
    return question


def init_model(config_dir: PathLike, loader) -> None:
    """Loads all model object files in a directory."""
    config_dir = Path(config_dir)
    with Session(_engine) as session:
        objs = []
        for filename in config_dir.glob("*.yaml"):
            with open(config_dir / filename) as f:
                config = yaml.safe_load(f)
                objs.append(loader(**config))

        session.add_all(objs)
        session.commit()


def init_db(engine: Engine = None) -> Engine:
    global _engine
    if engine:
        _engine = engine
    Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)
    init_model(sources_dir, _load_source)
    init_model(geogs_dir, _load_geog)

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

    init_model(questions_dir, _load_question)
    return _engine

from os import PathLike
from pathlib import Path

import click

from spacerat import SpaceRAT
from spacerat.config import init_db
from spacerat.scripts.generate_questions import generate_questions_for_source


def _highlight(text: str, **override) -> str:
    return click.style(text, fg="bright_blue", **override)


def _dimmed(text: str, **override) -> str:
    return click.style(text, fg="bright_black", **override)


def _bold(text: str, **override) -> str:
    return click.style(text, bold=True, **override)


def _italic(text: str, **override) -> str:
    return click.style(text, italic=True, **override)


def _spacerat():
    text = "SpaceRAT"
    colors = [
        (253, 231, 37),
        (189, 223, 38),
        (122, 209, 81),
        (68, 191, 112),
        (34, 168, 132),
        (33, 145, 140),
        (42, 120, 142),
        (53, 95, 141),
    ]
    result = ""

    for i, char in enumerate(text):
        result += click.style(
            char, fg=colors[i % len(colors)], bold=True, underline=True
        )
    return result


def write_or_print(text: str, filename: PathLike = None) -> None:
    if filename:
        with open(filename, "w") as f:
            f.write(text)
    else:
        print(text)


def abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()


def validate_write_setup(rat: SpaceRAT) -> bool:
    if not rat.source_write_url:
        click.echo(
            click.style(
                "⚠️ No changes made. Write access to source database required for geographic database commands.",
                fg="yellow",
            )
        )
        click.echo(_bold("\nSkipped.", fg="bright_yellow"))
        return False
    return True


@click.group()
@click.option(
    "-d",
    "--db",
    help="Database connection string for ontology storage",
    default=None,
)
@click.option(
    "--source-read-url",
    help="Database connection string for read access to source data.  Will use $SPACERAT_DATASTORE_READ_URL if not provided.",
    default=None,
)
@click.option(
    "--source-write-url",
    help="Database connection string for write access to source data.  Will use $SPACERAT_DATASTORE_WRITE_URL if not provided.",
    default=None,
)
@click.option(
    "--schema",
    help="Postgres schema for SpaceRAT-generated db objects. If not provided, will use $SPACERAT_SCHEMA or, failing that, 'spacerat'.",
    default=None,
)
@click.option(
    "--src",
    help="Path to to model files directory. If not provided, will use SPACERAT_MODEL_DIR or, failing that, './models'.",
    default=None,
)
@click.option("--debug", is_flag=True, default=False)
@click.option("--update", "-u", is_flag=True, default=False)
@click.pass_context
def cli(
    ctx: click.Context,
    db: str | None,
    source_read_url: str | None,
    source_write_url: str | None,
    schema: str | None,
    src: str | None,
    debug: bool,
    update: bool,
):
    """Root of CLI commands"""
    ctx.ensure_object(dict)
    args = {
        "db_url": db,
        "source_read_url": source_read_url,
        "source_write_url": source_write_url,
        "schema": schema,
        "model_dir": src,
        "skip_init": not update,
    }

    click.echo()
    click.echo(_spacerat())
    click.echo(_italic("Spatial-Relation Aggregation Toolkit\n"))

    rat = SpaceRAT(**{k: v for k, v in args.items() if v is not None}, debug=debug)

    if debug:
        click.echo(_bold("Running with the following settings"))
        for k in args.keys():
            click.echo("  " + _bold(k + ": ") + _dimmed(f"{getattr(rat, k)}"))

    ctx.obj["rat"] = rat


@cli.command()
@click.argument("source_id")
@click.pass_context
def generate_questions(ctx: click.Context, source_id: str):
    """
    Generate set of basic Questions from a table in the source database.

    This will generate one Question per column of the table and dump yaml representations of them in the model directory.
    """
    rat: SpaceRAT = ctx.obj["rat"]
    source = rat.get_source(source_id)

    click.echo(f"Dumping question files to {rat.model_dir}/{source.id}")

    generated = generate_questions_for_source(source, rat)

    click.echo(_highlight(str(generated)) + " question files generated")
    click.echo(_bold("\nDone!", fg="green"))


@cli.command()
@click.argument("src", required=False)
@click.option("--replace", "-r", is_flag=True, default=False)
@click.pass_context
def load_model(ctx: click.Context, src: PathLike, replace: bool):
    """
    Load model data from files in `SRC` into database.

    Will overwrite conflicting data.
    """
    rat: SpaceRAT = ctx.obj["rat"]
    init_db(rat.engine, src or rat.model_dir, drop=replace)
    click.echo(_bold("\nDone!", fg="green"))


@cli.command()
@click.argument("dst", type=click.Path(exists=True), required=False)
@click.pass_context
def dump_model(ctx: click.Context, dst: PathLike):
    """
    Dump state of model to yaml files in DST.

    It will add directories if needed and will overwrite conflicting files.
    """
    rat: SpaceRAT = ctx.obj["rat"]
    dst = Path(dst) if dst else None

    if dst:
        (dst / "geographies").mkdir(exist_ok=True)
        (dst / "sources").mkdir(exist_ok=True)
        (dst / "questions").mkdir(exist_ok=True)

    click.echo(_bold("Dumping geographies", fg="blue"))
    for geog in rat.get_geogs():
        write_or_print(
            geog.as_yaml(), dst / "geographies" / f"{geog.id}.yaml" if dst else None
        )

    click.echo(_bold("Dumping sources", fg="blue"))
    for source in rat.get_sources():
        write_or_print(
            source.as_yaml(), dst / "sources" / f"{source.id}.yaml" if dst else None
        )

    click.echo(_bold("Dumping questions", fg="blue"))
    for question in rat.get_questions():
        out_dir = None
        if dst:
            out_dir = dst / "questions" / question.source.id
            out_dir.mkdir(parents=True, exist_ok=True)

        write_or_print(
            question.as_yaml(), out_dir / f"{question.id}.yaml" if out_dir else None
        )

    click.echo(_bold("\nDone!", fg="green"))


@cli.command()
@click.argument("geog_levels", nargs=-1)
@click.option(
    "--yes",
    is_flag=True,
    callback=abort_if_false,
    expose_value=False,
    prompt="Drop and rebuild geographic index tables?",
)
@click.pass_context
def build_geo_indices(ctx: click.Context, geog_levels: tuple[str]):
    """Create/update materialized views for geographic indices using `Geography.query`."""
    rat: SpaceRAT = ctx.obj["rat"]

    if validate_write_setup(rat):
        for geog_level in geog_levels:
            click.echo(
                "Creating index table for " + _highlight(geog_level) + "...  ",
                nl=False,
            )
            rat.create_geog_index(geog_level)
            click.echo(_bold("Done!"))
        click.echo(_bold("Done!", fg="green"))


@cli.command()
@click.pass_context
def link_geogs(ctx: click.Context):
    rat: SpaceRAT = ctx.obj["rat"]
    click.echo("Creating geography linking tables")
    rat.create_geog_association_tables()
    click.echo(_bold("Done!", fg="green"))


@cli.command()
@click.argument("map_id", nargs=1)
@click.option(
    "--replace",
    "-r",
    is_flag=True,
    show_default=True,
    default=True,
    help="Overwrite tables if necessary.",
)
@click.option(
    "--yes",
    is_flag=True,
    callback=abort_if_false,
    expose_value=False,
    prompt="Drop and rebuild map tables?",
)
@click.pass_context
def update_maps(ctx: click.Context, map_id: str, replace):
    """Loads/updates maps for a MapConfig"""
    rat: SpaceRAT = ctx.obj["rat"]
    map_config = rat.get_map_config(map_id)
    click.echo("Creating maps using config: " + _highlight(map_config.name))

    if validate_write_setup(rat):
        rat.update_maps(map_id, replace=replace)

    click.echo(_bold("Done!", fg="green"))


@cli.command()
@click.argument("source_id", nargs=1)
@click.argument("geog_levels", nargs=-1)
@click.option("--include", "-i", help="Include a field", multiple=True)
@click.option("--exclude", "-x", help="Exclude a field", multiple=True)
@click.option(
    "--replace",
    "-r",
    is_flag=True,
    show_default=True,
    default=True,
    help="Overwrite tables if necessary.",
)
@click.option(
    "--yes",
    is_flag=True,
    callback=abort_if_false,
    expose_value=False,
    prompt="Drop and rebuild map tables?",
)
@click.pass_context
def populate_maps(
    ctx: click.Context,
    source_id: str,
    geog_levels: tuple[str],
    include: tuple[str],
    exclude: tuple[str],
    replace: bool,
):
    """
    Create or update materialized views used for indicator maps. These can then be served as vector tiles for mapping
    applications.

    This will create/update materialized a materialized view for each geog level provided with data from the provided
    source.

    Questions can be specified by passing lists of question IDs to `--include` or `--exclude`. If no specifications
    are made, all questions for the source will be used.
    """
    rat: SpaceRAT = ctx.obj["rat"]

    if validate_write_setup(rat):
        click.echo("Creating maps using data from source: " + _highlight(source_id))
        for geog_level in geog_levels:
            click.echo(
                "  • at " + _highlight(geog_level) + " level... ",
                nl=False,
            )
            rat.create_map_table(
                geog_level,
                source_id,
                included_questions=include,
                excluded_questions=exclude,
                replace=replace,
            )
            click.echo(_bold("Done!"))
        click.echo(_bold("Done!", fg="green"))


@cli.command()
@click.option(
    "--skip-maps",
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip loading maps.",
)
@click.pass_context
def init(ctx: click.Context, skip_maps: bool):
    """
    Initialize a SpaceRAT configration.

    This will...
    1. set up the SpaceRAT database,
    2. load your model from files, and
    3. make any necessary modifications on the source database. (e.g. creating a `spacerat` schema, creating geographic
       indices)
    """
    rat: SpaceRAT = ctx.obj["rat"]
    rat.reinit(skip_maps=skip_maps)

    click.echo(_bold("Done!", fg="green"))

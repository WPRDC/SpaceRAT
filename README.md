# 🚀🐀 SpaceRAT

**Spatial**-**R**elational **A**ggregation **T**oolkit

Series of tools to operationalize a semantic mapping from a developer-centered civic-data ontology to its representation
in a database.

Which is a bunch of fancy words for: it makes a system that can answer statistical questions about a place using a
small, standardized, and more-human-friendly vocabulary.

## How does SpaceRAT work?

1. It **defines a hierarchy of geographies**. This includes administrative regions or any set of similar features (e.g.
   parcels, 311 request points), 
2. **Defines where and how to retrieve the set of regions** (and their properties) within each feature and represent them in
   a standard fashion, and
3. **Defines how regions are described by the source data.** Allows for arbitrary queries for properties of regions. (e.g.
   median age of a neighborhood, price of a parcel, avg parcel price in a town), 
4. which allows it to **provide a standardized way for making queries using spatial aggregation without the need to dig through the
   underlying data.**

## Why not just structure your data like that in the first place?

SpaceRAT was designed with open data portals in mind (and particularly regional ones). These portals share data about a
single place or region but from various organizations, departments, or vendors who each may have their own data
publishing requirements.

SpaceRAT allows such portals to give their publishers flexibility while making it easy to unify their data.

## Installation

1. Create `spacerat` database
    - default is in-memory SQLite, can use any SQL database to store model.
    - can be on the source data postgres database within its own schema (default `spacerat`). This is necessary for full
      vector tile support.
2. Load/update model
3. Create/update materialized views for geographic indices using `Geography.query`.

## ⌨️ CLI

SpaceRAT provides a CLI tool to run useful maintenance scripts.

### generate-questions

Generate set of basic `Questions` from a table in the source database to be manually tweaked if necessary.

This will generate one `Question` per column of the table and dump yaml representations of them in the `model`
directory.

#### Usage

```shell
$ spacerat generate-questions source_id geog_level
```

#### Examples

Using the source [property-assessments](model/sources/property-assessments.yaml)

```shell
$ spacerat generate-questions property-assessments parcel
```

### update-model

Load model data from file into database. Will overwrite existing data on collision.

#### Usage

```shell
$ spacerat update-model [model_dir]
```

#### Examples

Load models from default directory (`./models`)

```shell
$ spacerat update-model
```

Load models from custom directory

```shell
$ spacerat update-model /path/to/dir/
```

#### Examples

Build/rebuild all geographic indices

```shell
$ spacerat update-model
```

Build/rebuild for `neighborhood` and `municipality` geographic levels

```shell
$ spacerat build-geo-indices neighborhood municipality
```

### build-geo-indices

Create/update materialized views for geographic indices using `Geography.query`

#### Usage

```shell
$ spacerat build-geo-indices [geog_level ...]
```

#### Examples

Build/rebuild all geographic indices

```shell
$ spacerat build-geo-indices
```

Build/rebuild for `neighborhood` and `municipality` geographic levels

```shell
$ spacerat build-geo-indices neighborhood municipality
```

### populate-maps

Create or update materialized views used for indicator maps. These can then be served as vector tiles for mapping
applications.

This will create/update materialized a materialized view for each geog level provided with data from the provided
source.

Questions can be specified by passing comma-separated lists of IDs to `--include` or `--exclude`. If no specifications
are made, all questions for the source will be used.

#### Usage

```shell
$ spacerat populate-maps source_id geog_level ... [--include=question_id ... --exclude=question_id ...]
```

#### Examples

Generate a map of `neighborhoods` with results for all `Questions` for `property-assessments`.

```shell
$ spacerat populate-maps property-assessments neighborhood
```

Generate maps for neighborhoods and municipalities with only values for `fairmarkettotal` (fair market assessed value
for land + building)  
***Note that `--exlcude` arguments will override any `--include` arguments.***

```shell
$ spacerat populate-maps property-assessments --include fairmarkettotal classdesc --exclude classdesc
```

### init

Initialize a SpaceRAT configration.

This will...

1. set up the SpaceRAT database,
2. load your model from files, and
3. make any necessary modifications on the source database. (e.g. creating a `spacerat` schema, creating geographic
   indices)


#### Usage

```shell
$ spacerat init [--skip-model --skip-geo-indices] 
```

## 🛜 API

### /answer

Query answers to `Questions` about `Regions`.

### /question/

List available `Questions`

### /question/<question_id>

Get details about a `Question`

### /geography/

List available geographic levels

### /geography/<geog_level>

Get details on a geographic level including a list of regions.

### /geograpy/<geog_level>/<region_id>

Get details on a specific region within a geographic level.

## 💻 Library

Use SpaceRAT as a library in your own code.

e.g.

```python
from spacerat.core import SpaceRAT
from spacerat.helpers import print_records

# initializing without args will use an in-memory sql db to hold the model,and will load the model
# from files found in ./model relative to the current working directory (not necessarily this file)
rat = SpaceRAT()

question = rat.get_question("fair-market-assessed-value")

neighborhoods = rat.get_geog("neighborhood")

shadyside = neighborhoods.get_region("shadyside")
bloomfield = neighborhoods.get_region("bloomfield")

print(question)
# Question(id='fair-market-assessed-value', name='Fair Market Assessed Value', datatype='continuous')

# Get stats on fair market assessment value for Shadyside at current time. 
print_records(rat.answer_question(question, shadyside))
# 2024-06-01T00:00:00
#   - mean: 516796.628695209
#   - mode: 250000.0
#   - min: 0.0
#   - first_quartile: 147300.0
#   - median: 260000.0
#   - third_quartile: 426775.0
#   - max: 126459400.0
#   - stddev: 2985002.463899841
#   - sum: 2027909971.0
#   - n: 3924


# Same thing but for Bloomfield
print_records(rat.answer_question(question, bloomfield))
# 2024-06-01T00:00:00
#   - mean: 228085.7476367803
#   - mode: 150000.0
#   - min: 0.0
#   - first_quartile: 66950.0
#   - median: 102700.0
#   - third_quartile: 171100.0
#   - max: 74945100.0
#   - stddev: 1596977.2907357663
#   - sum: 796247345.0
#   - n: 3491

# Shadyside again but only Residential parcels
print_records(rat.answer_question(question, shadyside, variant="residential"))
# 2024-06-01T00:00:00
#   - mean: 300894.54238310707
#   - mode: 200000.0
#   - min: 0.0
#   - first_quartile: 146000.0
#   - median: 245000.0
#   - third_quartile: 383350.0
#   - max: 2500000.0
#   - stddev: 238512.67504200496
#   - sum: 997465408.0
#   - n: 3315


# All 90 Pittsburgh neighborhoods, two parcel questions
print_records(
    rat.answer_question(
        ["parcel-class", "fair-market-assessed-value"], "neighborhood"
    )
)
# neighborhood.allegheny center
#   - time: 2024-07-01 00:00:00
#   - parcel_class__mode: COMMERCIAL
#   - parcel_class__n: 63
#   - fair_market_assessed_value__mean: 3949404.761904762
#   - fair_market_assessed_value__mode: 0.0
#   - fair_market_assessed_value__min: 0.0
#   - fair_market_assessed_value__first_quartile: 33450.0
#   - fair_market_assessed_value__median: 548800.0
#   - fair_market_assessed_value__third_quartile: 5588800.0
#   - fair_market_assessed_value__max: 22108700.0
#   - fair_market_assessed_value__stddev: 5999592.32838388
#   - fair_market_assessed_value__sum: 248812500.0
#   - fair_market_assessed_value__n: 63
# neighborhood.allegheny west
#   - time: 2024-07-01 00:00:00
#   - parcel_class__mode: RESIDENTIAL
#   - parcel_class__n: 269
#   - fair_market_assessed_value__mean: 546734.9442379182
#   - fair_market_assessed_value__mode: 54000.0
#   - fair_market_assessed_value__min: 0.0
#   - fair_market_assessed_value__first_quartile: 117300.0
#   - fair_market_assessed_value__median: 203400.0
#   - fair_market_assessed_value__third_quartile: 324400.0
#   - fair_market_assessed_value__max: 28230300.0
#   - fair_market_assessed_value__stddev: 2126818.841172308
#   - fair_market_assessed_value__sum: 147071700.0
#   - fair_market_assessed_value__n: 269
# neighborhood.allentown
#   - time: 2024-07-01 00:00:00
#   - parcel_class__mode: RESIDENTIAL
#   - parcel_class__n: 1547
#   - fair_market_assessed_value__mean: 42152.47899159664
#   - fair_market_assessed_value__mode: 400.0
#   - fair_market_assessed_value__min: 0.0
#   - fair_market_assessed_value__first_quartile: 3300.0
#   - fair_market_assessed_value__median: 20000.0
#   - fair_market_assessed_value__third_quartile: 32600.0
#   - fair_market_assessed_value__max: 10122900.0
#   - fair_market_assessed_value__stddev: 276475.42076201265
#   - fair_market_assessed_value__sum: 65209885.0
#   - fair_market_assessed_value__n: 1547
# ...
```


## Setup db
```postgresql
grant usage on schema spacerat to datastore;

grant select on all tables in schema spacerat to datastore;
```
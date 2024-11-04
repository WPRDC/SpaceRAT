
# initialize the spacerat model from our yaml file
spacerat init

# build geographic index tables for the geographies we use
spacerat build-geo-indices parcel --yes

# make geography linkin tables
spacerat link-geogs


# SpaceRAT is now usable

# make maps from assessment data
spacerat update-maps property-assessments --yes

# SQL statements that need to be integrated
# CREATE INDEX ON spacerat.parcel_index (parcel_id text_pattern_ops);
# CREATE INDEX ON spacerat.parcel_index (address text_pattern_ops);


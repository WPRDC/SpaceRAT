
# initialize the spacerat model from our yaml file
spacerat init

# build geographic index tables for the geographies we use
spacerat build-geo-indices blockgroup county county-subdivision neighborhood parcel school-district state-house state-senate tract us-house --yes

# make geography linkin tables
spacerat link-geogs


# SpaceRAT is now usable

# make maps from assessment data
spacerat update-maps property-assessments --yes
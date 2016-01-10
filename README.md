# Ambry Censuslib


Support code for building censys bundles in Ambry.


## Adding a New Census Year


- Update census.gov-acs_geofile-schemas-2009e
- Create census.gov-acs-geofile-<year>, the Geofile bundle
- Create census.gov-acs-p<release>ye<year>, the Census bundle

### Updating the Geofile Schemas, census.gov-acs_geofile-schemas-2009e

For each year, add to the sources:

- 2014-gs: Create a new sheet in the Google spreadsheet, with the schema for the geofile for the year

For each release:

- 2014_1_column_meta: Metadata from Census Reporter
- 2014_1_table_meta: Metadata from Census Reporter
- 2014-1-sequence: Sequence lookups, from the Census site
- 2014-1-shell: Table Shells, from the Census site


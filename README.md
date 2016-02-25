# Ambry Censuslib


## How to add a new census Year

### 1) Update the geofile bundles for the year. 


- Update census.gov-acs_geofile-schemas-2009e to include the geofile schemas for the target year
- Create census.gov-acs-geofile-<year>, the Geofile bundle
- Create census.gov-acs-p<release>ye<year>, the Census bundle

### 2)  Updating the Geofile Schemas, census.gov-acs_geofile-schemas-2009e

For each year, add to the sources:

- 2014-gs: Create a new sheet in the Google spreadsheet, with the schema for the geofile for the year

For each release:

- 2014_1_column_meta: Metadata from Census Reporter
- 2014_1_table_meta: Metadata from Census Reporter
- 2014-1-sequence: Sequence lookups, from the Census site
- 2014-1-shell: Table Shells, from the Census site


### 3) Add the base sources in the new census year bundle. 

Look at an existing bundle to get an idea of what these are. 

- base_url
- large_area_url
- small_area_url
- geofile
- table_sequence
- states


### 4) Build the schema

The create_table_schema actually adds both the sources and the schemas. 

    $ bambry exec create_table_schema  
    $ bambry sync -o 

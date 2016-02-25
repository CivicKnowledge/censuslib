""" An Ambry bundle for building geofile bundles

How to Create a New Geofile Bundle
==================================

Step 1: Setup source.

It's easiest to copy from a similar bundle and edit. You'll need these
sources:

* A dnlpage<year><release> for each year/release. These are URLs to the
  download page for the release
* geofile_schema, a ref to a geofile schema partition, such as
  census.gov-acs_geofile-schemas-2009e-geofile_schema-2013
* sumlevels, a partition ref to the list of summar levels, such as:
  census.gov-acs_geofile-schemas-2009e-sumlevels-none

Then there should be two or three 'split' sources, both at stage 2, one for
each release / year:

* geofile-split-20141
* geofile-split-20141


Step 2: build the source and dest schemas

    $ bambry exec meta_mkschema

Run 'bambry dump -T' to verify that a soruce schema was generated, and
'bambry dump -C' for the dest schema.

Step 3:  Add source links

    $ bambry exec meta_add_sources

Run 'bambry dump -s' to verify that a lost of new sources were generated.

Step 4: Update the datatype based on a single ingestion

    $ bambry exec meta_update_source_types

This will run for a while, because the California file is big. A big file should have the full
range of values for each column, so it is better for determining column datatypes

Run 'bambry dump -C' and verify that not all of the colums have the same datatype.

Step 5: Now, sync out and build the bundle

    $ bambry sync -o
    $ bambry -m build

This build will fail, because stage 2 expects the tables that are created in step 6. The error is:

    CRITICAL: No partition for ref: 'census.gov-acs-geofile-2013-geofile-20135'


Step 6: Construct the reduced schemas

After running, the 'meta_build_reduced_schemas' function can analyze the geofiles for each summary level
to determine which columns are used and which are not. Then it will create new schemas for each summary level

    $ bambry exec meta_build_reduced_schemas
    $ bambry sync -o

Run 'bambry dump -t' to verify there are many more tables


Step 7: Build again with stage 2, reduced schemas.

    bambry -m build

"""

import ambry.bundle

class GeofileBundle(ambry.bundle.Bundle):

    year = None

    def init(self):
        self._sl_map = None

    @staticmethod
    def non_int_is_null(v):

        try:
            return int(v)
        except ValueError:
            return None

    def geobundle_doc(self):
        """Print the docstring"""
        from censuslib.geofile import __doc__ as geobundle_docstring

        print geobundle_docstring


    ##
    ## Meta, Step 2: build the source and dest schemas
    ##
    def meta_mkschema(self):
        """Create the  geofile schema from the configuration in the
           upstream bundle. """
        from ambry.orm.file import File

        t = self.dataset.new_table('geofile')
        st = self.dataset.new_source_table('geofile')

        p = self.dep('geofile_schema')
        i = 1
        for row in p:

            if  row['year'] == self.year :
                i += 1
                name = row['name'].lower().strip()
                name = name if name != 'blank' else 'blank{}'.format(i)
                self.logger.info(name)

                t.add_column(name, datatype = 'str', description = row['description'])

                st.add_column( source_header = name, position = row['seq'],
                        datatype = str,
                        start = row['start'], width = row['width'],
                        description = row['description'])

        self.commit()

        self.build_source_files.sourceschema.objects_to_record()
        self.build_source_files.schema.objects_to_record()

        self.commit()

    ##
    ## Meta Step 3: Add source links
    ##
    def meta_add_sources(self):
        self._meta_add_13yr_sources(span=1)
        if self.year <= 2013:
            self._meta_add_13yr_sources(span=3)

        self._meta_add_5yr_sources()


    def _meta_add_13yr_sources(self, span):
        """Run once to create to create the sources.csv file. Scrapes the web page with the links to the
        files.  """
        from ambry.orm import DataSource, File
        from ambry.util import scrape_urls_from_web_page
        from ambry.orm.exc import NotFoundError


        source = self.source('dnlpage{}{}'.format(self.year,span))

        entries = scrape_urls_from_web_page(source.url)['sources']

        for k,v in entries.items():

            d = {
                'name': k.lower()+"_{}{}".format(self.year,span),
                'source_table_name': 'geofile',
                'dest_table_name': 'geofile',
                'filetype': 'csv',
                'file': 'g{}.*\.csv'.format(self.year),
                'encoding': 'latin1',
                'time': str(self.year)+str(span),
                'start_line': 0,
                'url': v['url']
            }

            try:
                s = self._dataset.source_file(d['name'])
                s.update(**d)
            except NotFoundError:
                s = self.dataset.new_source(**d)

            self.session.merge(s)


        self.commit()

        self.build_source_files.sources.objects_to_record()

        self.commit()


    def _meta_add_5yr_sources(self):
        """The 5 year release has a different structure because the files are bigger. """
        from ambry.orm import DataSource, File
        from ambry.util import scrape_urls_from_web_page
        from ambry.orm.exc import NotFoundError
        import os

        year = self.year
        span = 5

        source = self.source('dnlpage{}{}'.format(year,span))

        self.log("Loading from {}".format(source.url))

        name_map={
            'All_Geographies_Not_Tracts_Block_Groups': 'L',
            'Tracts_Block_Groups_Only': 'S'
        }

        def parse_name(inp):
            for suffix, code in name_map.items():
                if inp.endswith(suffix):
                    return inp.replace('_'+suffix, ''), code
            return (None, None)


        for link_name, parts in scrape_urls_from_web_page(source.url)['sources'].items():
            url=parts['url']

            state_name, size_code = parse_name(link_name)

            d = {
                'name': "{}{}_{}{}".format(state_name,size_code,self.year, span),
                'source_table_name': 'geofile',
                'dest_table_name': 'geofile',
                'filetype': 'csv',
                'file': 'g{}.*\.csv'.format(self.year),
                'encoding': 'latin1',
                'time': str(self.year)+str(span),
                'start_line': 0,
                'url':url
            }

            try:
                s = self._dataset.source_file(d['name'])
                s.update(**d)
            except NotFoundError:
                s = self.dataset.new_source(**d)

            self.session.merge(s)
            self.log(s.name)

        self.commit()

        self.build_source_files.sources.objects_to_record()

        self.commit()

    ##
    ## Meta Step 4: Update the datatype based on a single ingestion
    ##
    def meta_update_source_types(self):
        from ambry_sources.intuit import TypeIntuiter

        source_name = 'CaliforniaS_{}5'.format(self.year)

        s = self.source(source_name)
        s.start_line = 0
        s.header_lines = []
        self.commit()

        self.ingest(sources=[s.name], force=True)

        s = self.source(s.name)
        st = self.source_table('geofile')
        dt = self.table('geofile')

        def col_by_pos(pos):
            for c in st.columns:
                if c.position == pos:
                    return c

        with s.datafile.reader as r:

            for col in r.columns:
                c = col_by_pos(col.position+1)

                c.datatype = col['resolved_type'] if col['resolved_type'] != 'unknown' else 'str'

                dc = dt.column(c.name)
                dc.datatype = c.datatype

        self.commit()

        self.build_source_files.sourceschema.objects_to_record()
        self.build_source_files.schema.objects_to_record()

        self.commit()


    ##
    ## Meta Step 6, After Build: Create per-summary level tables
    ##
    def meta_build_reduced_schemas(self):
        """
        After running once, it is clear that not all columns are used in all
        summary levels. This routine builds new tables for all of the summary
        levels that have only the columns that are used.


        """
        from collections import defaultdict
        from itertools import islice, izip

        table_titles = { int(r['sumlevel']): r['description'] if r['description'] else r['sumlevel']
                         for r in self.dep('sumlevels')}

        p = self.partition(table='geofile', time='{}5'.format(self.year))


        # Create a dict of sets, where each set holds the non-empty columns for rows of
        # a summary level
        gf = defaultdict(set)
        for r in p:
            gf[r.sumlevel] |= set(k for k,v in r.items() if v)

        for sumlevel, fields in gf.items():

            t = self.dataset.new_table('geofile'+str(sumlevel))
            t.columns = []
            self.commit()

            t.description = 'Geofile for: ' + str(table_titles.get(int(sumlevel), sumlevel))

            self.log('New table {}: {}'.format(t.name, t.description))

            for c in self.table('geofile').columns:
                if c.name in fields:
                    t.add_column(name=c.name, datatype=c.datatype, description=c.description, transform=c.transform)

        self.commit()

        self.build_source_files.schema.objects_to_record()

        self.commit()

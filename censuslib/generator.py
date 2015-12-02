# Table generators

from ambry.util import memoize

class ACS09TableRowGenerator(object):
    """Generate table rows by combining multuple state files, slicing out 
    an individual table, and merging the estimates and margins"""
    
    def __init__(self, bundle, source):
        self.source = source
        self.bundle = bundle
        self.library = self.bundle.library
        self.year = int(self.bundle.year)
        self.release = int(self.bundle.release)
        self.header_cols = self.bundle.header_cols
        self.url_root = self.bundle.source('base_url').ref

        self.large_url_template = self.bundle.source('large_area_url').ref
        self.small_url_template = self.bundle.source('small_area_url').ref

        
        self.limited_run = self.bundle.limited_run

        self._states = None

    @property
    @memoize
    def states(self):
        """Return tuples of states, which can be used to make maps and lists"""
        
        if not self._states:
        
            self._states = []
        
            with self.bundle.dep('states').reader as r:
                for row in r.select( lambda r: r['component'] == '00'):
                    self._states.append((row['stusab'], row['state'], row['name'] ))


        if self.bundle.limited_run:
            return self._states[:3]
        else:
            return self._states

    def generate_source_specs(self):
        """Generate fake source specs for all of the files that underlie this table, which
        are actually the same for every table. """

        from ambry_sources import SourceSpec

        table = self.source.dest_table

        if isinstance(table, str):
            table = self.table(table)

        sequence = int(table.data['sequence'])

        for stusab, state_id, state_name in self.states:
            file = "{}{}{}{:04d}000.txt".format(self.year, self.release,
                                                stusab.lower(), sequence)

            templates = []

            if self.small_url_template:
                templates.append(('s', self.small_url_template))

            if self.large_url_template:
                templates.append(('l', self.large_url_template))

            for (size, url_template) in templates:

                url = url_template.format(root=self.url_root, state_name=state_name).replace(' ', '')

                spec1 = SourceSpec(
                    url=url,
                    filetype='csv',
                    reftype='zip',
                    file='e' + file
                )

                spec2 = SourceSpec(
                    url=url,
                    filetype='csv',
                    reftype='zip',
                    file='m' + file
                )

                yield (spec1, spec2)

    def __iter__(self):

        from ambry_sources import get_source
        from itertools import izip, chain
        from ambry.etl import Slice
        from ambry.orm import Column
        
        table = self.source.dest_table
        
        if isinstance(table, str):
            table = self.table(table)

        start = int(table.data['start'])
        length = int(table.data['length'])

        slca_str = ','.join(str(e[4]) for e in self.header_cols)
        slcb_str =  "{}:{}".format(start-1, start+length-1)
        
        # Slice for the stusab, logrecno, etc. 
        slca, slc_code = Slice.make_slicer(slca_str)
        # Slice for the data columns
        slcb, slc_code = Slice.make_slicer(slcb_str)
        
        columns = [ c.name for c in table.columns ]

        # Columns before the first data column, by removing the
        # data columns, which are presumed to all be at the end. 
        preamble_cols = columns[:-2*len(slcb(range(1,300)))]
        data_columns =  columns[len(preamble_cols):]
        
        header_cols = [e[0] for e in self.header_cols]

        # A few sanity checks
        assert preamble_cols[-1] == 'jam_flags'
        assert data_columns[0][-3:] == '001'
        assert data_columns[1][-3:] == 'm90'

        all_cols =  [ Column.mangle_name(c) for c in header_cols + data_columns]

        yield all_cols

        cache = self.library.download_cache

        row_n = 0
        for spec1, spec2 in self.generate_source_specs():

            s1 = get_source(spec1, cache)
            s2 = get_source(spec2, cache)

            for i, (row1, row2) in enumerate(izip(s1, s2)):
                # Interleave the slices of the of the data rows, prepend
                # the stusab, logrecno, etc.

                row_n += 1
                if self.limited_run and row_n > 10000:
                    return

                yield slca(row1)+tuple(chain(*zip(slcb(row1),slcb(row2))))
                    
        
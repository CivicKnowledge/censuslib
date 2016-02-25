# Support for building census bundles in Ambry

__version__ = 0.8
__author__ = 'eric@civicknowledge.com'


from .generator import * 
from .schema import *
from .sources import * 
from .transforms import *

import ambry.bundle

class AcsBundle(ambry.bundle.Bundle, MakeTablesMixin, MakeSourcesMixin,
             JamValueMixin, JoinGeofileMixin):
    # Which of the first columns in the data tables to use.
    header_cols = [
        # Column name, Description, width, datatype, column position
        #('FILEID','File Identification',6,'str' ),
        #('FILETYPE','File Type',6,'str'),
        ('STUSAB','State/U.S.-Abbreviation (USPS)',2,'str',2 ),
        ('CHARITER','Character Iteration',3,'str',3 ),
        ('SEQUENCE','Sequence Number',4,'int',4 ),
        ('LOGRECNO','Logical Record Number',7,'int',5 )
    ]

    def init(self):
        from .util import year_release

        self.year, self.release = year_release(self)

        self.log("Building Census bundle, year {}, release {}".format(self.year, self.release))

    def edit_pipeline(self, pipeline):
        """Change the SelectPartitionFromSource so it only writes a single partition"""
        from ambry.etl import SelectPartitionFromSource

        # THe partition is named only after the table.
        def select_f(pipe, bundle, source, row):
            return source.dest_table.name

        pipeline.select_partition = SelectPartitionFromSource(select_f)


    @CaptureException
    def _pre_download(self, gen_cls):
        """Override the ingestion process to download all of the input files at once. This resolves
        the contention for the files that would occurr if many generators are trying to download
        the same files all at once. """
        from ambry_sources import download

        cache = self.library.download_cache

        source = self.source('b00001') # First; any one will do

        g = gen_cls(self, source)

        downloads = []

        for spec1, spec2 in g.generate_source_specs():

            downloads.append( (spec1.url, cache) )

            # The two specs usually point to different files in the same zip archive, but I'm not sure
            # that is always true.
            if spec1.url != spec2.url:
                downloads.append((spec2.url, cache))

        # Multi-processing downloads might improve the speed, although probably not by much.
        for url, cache in downloads:
            self.log("Pre-downloading: {}".format(url))
            download(url, cache)

class ACS2009Bundle(AcsBundle):
    pass

class ACS2010Bundle(AcsBundle):

    @CaptureException
    def ingest(self, sources=None, tables=None, stage=None, force=False, update_tables=True):
        """Override the ingestion process to download all of the input files at once. This resolves
        the contention for the files that would occurr if many generators are trying to download
        the same files all at once. """

        from.generator import ACS09TableRowGenerator

        self._pre_download(ACS09TableRowGenerator)

        return super(ACS2010Bundle, self).ingest(sources, tables, stage, force, update_tables)

import unittest


class TestDimensions(unittest.TestCase):

    def test_dump_all(self):
        from ambry import get_library
        from censuslib.dimensions import classify

        l = get_library()

        p = l.partition('census.gov-acs-p5ye2014-b01001')

        for c in p.table.columns:
            if c.name == 'id':
                continue
            if not c.name.endswith('_m90'):
                    print c.name, classify(c)

    def test_basic(self):
        from ambry import get_library

        l = get_library()

        p = l.partition('census.gov-acs-p5ye2014-b01001')

        df = p.dataframe()

        print df.dim()


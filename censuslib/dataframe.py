"""
Pandas dataframes for Census tables.

"""

from ambry.pands import AmbryDataFrame, AmbrySeries
import numpy as np
from six import string_types


def melt(df):
    """Melt a census dataframe into two value columns, for the estimate and margin"""
    import pandas as pd

    # Intial melt
    melted = pd.melt(df, id_vars=list(df.columns[:9]), value_vars=list(df.columns[9:]))
    melted = melted[['gvid', 'variable', 'value']]

    # Make two seperate frames for estimates and margins.
    estimates = melted[~melted.variable.str.contains('_m90')].set_index(['gvid', 'variable'])
    margins = melted[melted.variable.str.contains('_m90')].copy()

    margins.columns = ['gvid', 'ovariable', 'm90']
    margins['variable'] = margins.ovariable.str.replace('_m90', '')

    # Join the estimates to the margins.
    final = estimates.join(margins.set_index(['gvid', 'variable']).drop('ovariable', 1))

    return final

class CensusSeries(AmbrySeries):

    ambry_column = None

    def __init__(self, data=None, index=None, dtype=None, name=None, copy=False, fastpath=False):
        super(CensusSeries, self).__init__(data, index, dtype, name, copy, fastpath)


    def m90(self):
        if self.name.endswith('_m90'):
            return self
        else:
            return self._dataframe[self.name+'_m90'].astype('float')

    def value(self):
        """Return the float value for an error column"""
        if self.name.endswith('_m90'):
            return self._dataframe[self.name.replace('_m90','')].astype('float')
        else:
            return self


    def se(self):
        """Return a standard error series, computed from the 90% margins"""
        return self.m90() / 1.645


    def rse(self):
        """Return the relative standard error for a column"""

        return self.se() / self.value() * 100

    def m95(self):
        """Return a standard error series, computed from the 90% margins"""
        return self.m90() / 1.645 * 1.96


    def m99(self):
        """Return a standard error series, computed from the 90% margins"""
        return self.m90() / 1.645 * 2.575


class CensusDataFrame(AmbryDataFrame):



    def lookup(self, c):
        from ambry.orm.exc import NotFoundError

        if isinstance(c, string_types):
            c =  self[c]
        if isinstance(c, int):
            suffix = str(c).zfill(3)
            full_col = [col for col in self.columns if col.endswith(suffix)][0]
            c = self[full_col]
        else:
            pass

        try:
            c.ambry_column = self.partition.table.column(c.name)
        except NotFoundError:
            c.ambry_column = None

        return c

    def sum_m(self, *cols):
        """Sum a set of Dataframe series and return the summed series and margin. The series must have names"""

        # See the ACS General Handbook, Appendix A, "Calculating MOEs for
        # Derived Proportions". (https://www.census.gov/content/dam/Census/library/publications/2008/acs/ACSGeneralHandbook.pdf)
        # for a guide to these calculations.

        # This is for the case when the numerator is a subset of the denominator

        # Convert string column names to columns.

        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]

        cols = [ self.lookup(c) for c in cols]

        value = sum(cols)

        m = np.sqrt(sum(c.m90()*c.m90() for c in cols))

        return value, m

    def add_sum_m(self, col_name, *cols):
        """
        Add new columns for the sum, plus error margins, for 2 or more other columns

        The routine will add two new columns, one named for col_name, and one for <col_name>_m90

        :param col_name: The base name of the new column
        :param cols:
        :return:
        """

        self[col_name], self[col_name+'_m90'] = self.sum_m(*cols)



    def add_rse(self, *col_name):
        """
        Create a new column, <col_name>_rse for Relative Standard Error, using <col_name> and <col_name>_m90

        :param col_name:
        :return:

        """

        for cn in col_name:
            self[cn + '_rse'] = self[cn].rse()

    def sum_col_group(self, header, last):
        """Sum a contiguous group of columns, and return the sum and the new margins.  """

        cols = [self.lookup(i) for i in range(header, last+1)]

        value = sum(cols)

        m = np.sqrt(np.sum(c.m90()**2 for c in cols))

        return value, m

    def ratio(self, n, d, subset=True):
        """
        Compute a ratio of a numerator and denominator, propagating errors

        Both arguments may be one of:
        * A Series, which must hav a .name property for a column in the dataset
        * A column name
        * A tuple of two of either of the above.

        In the tuple form, the first entry is the value and the second is the 90% margin

        :param n: A series or tuple(Series, Series)
        :param d: A series or tuple(Series, Series)
        :return: Tuple(Series, Series)
        """

        def normalize(x):
            if isinstance(x, tuple):
                x, m90 = self.lookup(x[0]), self.lookup(x[1])
            elif isinstance(x, string_types):
                x = self.lookup(x)
                m90 = x.m90()
            elif isinstance(x, AmbrySeries):
                m90 = x.m90()
            elif isinstance(x, int):
                x = self.lookup(x)
                m90 = x.m90()

            return x, m90

        n, n_m90 = normalize(n)
        d, d_m90 = normalize(d)

        rate = np.round(n / d, 3)

        if subset:
            try:
                # From external_documentation.acs_handbook, Appendix A, "Calculating MOEs for
                # Derived Proportions". This is for the case when the numerator is a subset of the
                # denominator
                rate_m = np.sqrt(n_m90 ** 2 - ((rate ** 2) * (d_m90 ** 2))) / d

            except ValueError:
                # In the case, of a neg arg to a square root, the acs_handbook recommends using the
                # method for "Calculating MOEs for Derived Ratios", where the numerator
                # is not a subset of the denominator. Since our numerator is a subset, the
                # handbook says " use the formula for derived ratios in the next section which
                # will provide a conservative estimate of the MOE."
                # The handbook says this case should be rare, but for this calculation, it
                # happens about 50% of the time.
                rate_m = np.sqrt(n_m90 ** 2 + ((rate ** 2) * (d_m90 ** 2))) / d

        else:
            rate_m = np.sqrt(n_m90 ** 2 + ((rate ** 2) * (d_m90 ** 2))) / d

        return rate, rate_m

    def dim_columns(self, pred):
        """
        Return a list of columns that have a particular value for age,
        sex and race_eth. The `pred` parameter is a string of python
        code which is evaled, with the classification dict as the local
        variable context, so the code string can access these variables:

        - sex
        - age
        - race-eth
        - col_num

        Col_num is the number in the last three digits of the column name

        Some examples of predicate strings:

        - "sex == 'male' and age != 'na' "

        :param pred: A string of python code that is executed to find column matches.

        """

        from censuslib.dimensions import classify


        out_cols = []

        for i, c in enumerate(self.partition.table.columns):
            if c.name.endswith('_m90'):
               continue

            if i < 9:
                continue

            cf = classify(c)
            cf['col_num'] = int(c.name[-3:])

            if eval(pred, {}, cf):
                out_cols.append(c.name)

        return out_cols

    def __getitem__(self, key):
        """

        """
        from pandas import DataFrame, Series
        from ambry.orm.exc import NotFoundError


        result = super(CensusDataFrame, self).__getitem__(key)

        if isinstance(result, DataFrame):
            result.__class__ = CensusDataFrame
            result._dataframe = self

        elif isinstance(result, Series):
            result.__class__ = CensusSeries
            result._dataframe = self
            try:
                result.ambry_column = self.partition.table.column(result.name)
            except NotFoundError:
                result.ambry_column = None

        return result

    def copy(self, deep=True):

        r =  super(CensusDataFrame, self).copy(deep)
        r.__class__ = CensusDataFrame
        r.partition = self.partition

        return r


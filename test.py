import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import pyarrow as pa

import unittest
import os
from pandas._testing import assert_frame_equal

class TestParquet(unittest.TestCase):

    def setUp(self):
        self.dataset = "Apple_Data_300.csv"
        self.dataset_test = "test.parquet"
        self.df = pd.read_csv(self.dataset)

    def test_write(self):

        table = pa.Table.from_pandas(self.df)

        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
        # There are several compression algorithms (Snappy, GZip, Brotli...) and they can be applied per-column basis.
        # i.e: pq.write_table(table, "test.parquet", compression={'col1': 'snappy', 'col2': 'gzip', ....})
        # NOTE: when working with Spark, a flag 'flavor' should be passed to write_table, to satisfy the constraints
        # imposed on types by Spark.
        # i.e: pq.write_table(table, "test.parquet", flavor='spark')
        pq.write_table(table, self.dataset_test)

        self.assertTrue(os.path.isfile(self.dataset_test))

    def test_read(self):

        df2 = pq.read_table(self.dataset_test).to_pandas()

        assert_frame_equal(self.df,df2)

    def test_size_reduction(self):

        dataset_size = os.stat(self.dataset).st_size
        dataset_test_size = os.stat(self.dataset_test).st_size

        self.assertGreaterEqual(dataset_size, dataset_test_size)

if __name__=='__main__':
    unittest.main()
    #print(pq.ParquetFile('test.parquet').metadata)
    #print(pq.ParquetFile('test.parquet').schema)
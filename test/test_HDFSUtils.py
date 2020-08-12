import unittest
from datetime import datetime

from py4j.protocol import Py4JJavaError

from utils.HDFSUtils import HDFSUtils


class HDFSUtilsTest(unittest.TestCase):
    def setUp(self):
        self.utils = HDFSUtils()
        self.path = "/data/master/pctk/data/t_pctk_rcc_balance"
        self.process_date = "2020-07-12"

    def test_init(self):
        self.assertEqual("cutoff_date", self.utils.partition_name)
        self.assertEqual("%Y-%m-%d", self.utils.date_format)

    def test__to_date(self):
        processed_date = self.utils._HDFSUtils__to_date(self.process_date)
        date = datetime(2020, 7, 12)
        self.assertEqual(date, processed_date)

    def test__get_files(self):
        files = self.utils._HDFSUtils__get_files(self.path)
        self.assertTrue(files)

    def test__get_files_with_extensions(self):
        path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30"
        files = self.utils.get_files(path)

        self.assertTrue(self.utils.get_files_with_extensions(files))

    def test__get_date(self):
        files = list(self.utils._HDFSUtils__get_files(self.path))
        path = self.utils._HDFSUtils__get_date(files[0])
        expected_date = '2020-04-30'
        self.assertEqual(path, expected_date)

    def test__get_date_when_path_not_found(self):
        path = "/path_not_found"
        files = []

        with self.assertRaises(Py4JJavaError) as java_error:
            files = list(self.utils._HDFSUtils__get_files(path))

        java_exception = java_error.exception
        self.assertEqual(java_exception.args[1].getMessage(), "File {path} does not exist.".format(path=path))

        with self.assertRaises(IndexError) as error:
            expected_path = self.utils._HDFSUtils__get_date(files[0])
        exception = error.exception
        self.assertEqual(exception.args[0], "list index out of range")

    def test__filter_dates(self):
        files = self.utils._HDFSUtils__get_files(self.path)
        filtered_dates = self.utils._HDFSUtils__filter_dates(files)
        self.assertTrue(filtered_dates)

    def test__sort_date_partitions(self):
        expected_partitions = self.utils._HDFSUtils__sort_date_partitions(self.path)
        self.assertTrue(sorted(expected_partitions) == expected_partitions)

    def test__format_date_partitions(self):
        sorted_date_partitions = self.utils._HDFSUtils__sort_date_partitions(self.path)
        formatted_date_partitions = self.utils._HDFSUtils__format_date_partitions(sorted_date_partitions)
        self.assertTrue(formatted_date_partitions)

    def test__filter_date_partitions(self):
        sorted_date_partitions = self.utils._HDFSUtils__sort_date_partitions(self.path)
        filtered_date_partitions = self.utils._HDFSUtils__filter_date_partitions(sorted_date_partitions,
                                                                                 self.process_date)
        self.assertTrue(filtered_date_partitions)

    def test_get_exception_with_invalid_path(self):
        path = "/data/master/pctk/data/t_pctk_test"
        self.assertRaises(Py4JJavaError, lambda: self.utils.get_date_partitions(path))
        with self.assertRaises(Py4JJavaError) as error:
            self.utils.get_date_partitions(path)
        exception = error.exception
        self.assertEqual(exception.args[1].getMessage(), "File {path} does not exist.".format(path=path))

    def test_get_master_with_date_partitions(self):
        partition_number = 1
        expected_partition = ['2020-04-30']
        last_partition = self.utils.get_date_partitions(self.path, self.process_date, partition_number)
        self.assertEqual(last_partition, expected_partition)

    def test_get_master_without_date_partitions(self):
        path = "/data/master/pdco/data/cross/v_pdco_geo_location_catalog/"
        date_partitions = self.utils.get_date_partitions(path)
        self.assertFalse(date_partitions)

    def test_get_raw_date_partitions(self):
        path = "/data/raw/pext/data/t_pext_rcc_balance/"
        partition_number = 3
        expected_partitions = ['20180731', '20180630', '20180531']
        date_partitions = HDFSUtils(date_format='%Y%m%d').get_date_partitions(path, partition_number)
        self.assertEqual(date_partitions, expected_partitions)

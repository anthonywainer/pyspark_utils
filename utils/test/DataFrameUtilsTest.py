import datetime
from unittest import TestCase

from pyspark.shell import sc, spark, sqlContext
from pyspark.sql.types import StructType, Row

from utils.DataFrameUtils import DataFrameUtils


class DataFrameUtilsTest(TestCase):
    def setUp(self):
        self.dataFrame = DataFrameUtils(spark, sc=sc)
        self.path = "/data/master/pctk/data/t_pctk_rcc_balance"
        self.process_date = "2020-07-12"

    def test__search_extension(self):
        file = "data/raw/part-00001-59364dc0-fc95-4467-851a-d1667a8be31d.c000.avro"
        extension = self.dataFrame._DataFrameUtils__search_extension(file)
        expected_extension = "avro"

        self.assertEqual(extension, expected_extension)

    def test__extract_extension(self):
        files = [
            "data/raw/part-00001-59364dc0-fc95-4467-851a-d1667a8be31d.c000.avro",
            "data/raw/part-00001-59364dc0-fc95-4467-851a-d1667a8be31d.c000.avro"
        ]
        extension = self.dataFrame._DataFrameUtils__extract_extension(files)
        expected_extension = "avro"

        self.assertEqual(extension, expected_extension)

    def test__extract_extension_with_invalid_extension(self):
        files = [
            "data/raw/part-00001-59364dc0-fc95-4467-851a-d1667a8be31d.c000.avro",
            "data/raw/part-00001-59364dc0-fc95-4467-851a-d1667a8be31d.c000.txt"
        ]

        with self.assertRaises(Exception) as error:
            self.dataFrame._DataFrameUtils__extract_extension(files)

        exception = error.exception
        self.assertEqual(exception.args[0], "Extension doesn't recognized!")

    def test_get_format_type_from_path(self):
        path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30/"
        extension = self.dataFrame.get_format_type_from_path(path)
        expected_extension = "parquet"

        self.assertEqual(extension, expected_extension)

    def test__get_format_type(self):
        path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30/"
        extension = self.dataFrame._DataFrameUtils__get_format_type(path)
        expected_extension = "parquet"

        self.assertEqual(extension, expected_extension)

    def test__extract_path(self):
        path = self.dataFrame._DataFrameUtils__extract_path(self.path)
        expected_path = "hdfs://pedaaswork.scmx2p100.isi/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-07-31"

        self.assertEqual(path, expected_path)

    def test__extract_path_with_partition_name(self):
        path = self.dataFrame._DataFrameUtils__extract_path(self.path + "/cutoff_date=2020-04-30/")
        expected_path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30/"

        self.assertEqual(path, expected_path)

    def test__extract_path_with_paths(self):
        paths = [
            "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30/",
            "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-05-31/"
        ]
        path = self.dataFrame._DataFrameUtils__extract_path(paths=paths)
        expected_path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30/"

        self.assertEqual(path, expected_path)

    def test__concat_path_with_partition_name(self):
        date_partitions = [
            "2020-04-30"
        ]
        path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date="

        partitions = self.dataFrame._DataFrameUtils__concat_path_with_partition_name(date_partitions, path)

        expected_partition = ["/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-04-30"]

        self.assertEqual(partitions, expected_partition)

    def test__add_partition_name(self):
        path = "/data/master/pctk/data/t_pctk_rcc_balance/"
        path = self.dataFrame._DataFrameUtils__add_partition_name(path)

        expected_path = "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date="

        self.assertEqual(path, expected_path)

    def test__get_paths_with_process_name(self):
        paths = self.dataFrame._DataFrameUtils__get_paths_with_process_name(self.path, partition_number=2)
        expected_paths = [
            "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-07-31",
            "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-06-30"
        ]

        self.assertEqual(paths, expected_paths)

    def test_read_dataframe(self):
        dataframe = self.dataFrame.read_dataframe(self.path)
        empty_dataframe = sqlContext.createDataFrame([], StructType([]))

        self.assertNotEqual(dataframe, empty_dataframe)

    def test_read_dataframe_with_path(self):
        paths = ["/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-06-30",
                 "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-07-31"]

        dataframe = self.dataFrame.read_dataframe(paths=paths)
        empty_dataframe = sqlContext.createDataFrame([], StructType([]))

        self.assertNotEqual(dataframe, empty_dataframe)

    def test_read_dataframe_with_path_retrieving_partition_name(self):
        paths = ["/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-06-30",
                 "/data/master/pctk/data/t_pctk_rcc_balance/cutoff_date=2020-07-31"]

        dataframe = self.dataFrame.read_dataframe(paths=paths,
                                                  options={'basePath': self.path})

        empty_dataframe = sqlContext.createDataFrame([], StructType([]))

        self.assertNotEqual(dataframe, empty_dataframe)
        self.assertTrue("cutoff_date" in dataframe.schema.names)

    def test_read_dataframes(self):
        dataframe = self.dataFrame.read_dataframes(self.path, partition_number=1)
        empty_dataframe = sqlContext.createDataFrame([], StructType([]))

        self.assertNotEqual(dataframe, empty_dataframe)

    def test_read_dataframes_with_date_range(self):
        dataframe = self.dataFrame.read_dataframes(self.path, process_date=["2020-05-31", "2020-07-31"],
                                                   options={"basePath": self.path})

        empty_dataframe = sqlContext.createDataFrame([], StructType([]))
        dates = dataframe.select("cutoff_date").dropDuplicates().collect()
        expected_dates = [Row(cutoff_date=datetime.date(2020, 7, 31)),
                          Row(cutoff_date=datetime.date(2020, 5, 31)),
                          Row(cutoff_date=datetime.date(2020, 6, 30))]

        self.assertNotEqual(dataframe, empty_dataframe)
        self.assertEqual(dates, expected_dates)

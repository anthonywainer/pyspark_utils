from typing import List
from pyspark.sql import DataFrame

import re

from utils.HDFSUtils import HDFSUtils


class DataFrameUtils(HDFSUtils):
    """Class that is used to work with dataFrames
    Extends:
        HDFSUtils
    """

    @property
    def format_types_spark(self):
        return {
            "avro": "com.databricks.spark.avro",
            "parquet": "parquet",
            "csv": "csv",
            "txt": "csv",
            "ctl": "csv",
            "dat": "csv"
        }

    def __init__(self, spark, **kwargs):
        self.slash = "/"
        self.spark = spark
        super().__init__(**kwargs)

    @staticmethod
    def __extract_extension(files: List[str]) -> str:
        """ Extracting extension in files
        :param files             The list files
        :return extension if the given files has had extensions, in any case, thrown Exception
        """
        extension_regex = re.compile("[^.]+$")
        extensions = [extension_regex.findall(file)[0] for file in files]
        if len(set(extensions)) == 1:
            return extensions[0]
        else:
            raise Exception("Extension doesn't recognized!")

    def get_format_type_from_path(self, path_name: str) -> str:
        """ Getting format type from path
        :param path_name         The path name
        :return type format if the given path has had files with extensions, in any case, thrown Exception
        """
        content = list(self.get_content(path_name))
        files = self._filter_files(content)
        return self.__extract_extension(files)

    def __get_format_type(self, path_name: str) -> str:
        """ Getting format type from dict
        :param path_name         The path name
        :return format type for spark
        """
        return self.format_types_spark[self.get_format_type_from_path(path_name)]

    def __extract_path(self, path_name: str = None, paths: List[str] = None) -> str:
        """ Extracting path
        :param path_name         The path name
        :param paths             The path list
        :return format type for spark
        """

        if paths is not None:
            path_name = paths[0]

        if self.partition_name in path_name:
            return path_name

        try:
            return self.get_folders(path_name)[0]
        except IndexError:
            return path_name

    @staticmethod
    def __concat_path_with_partition_name(date_partitions: List[str], path_name: str) -> List[str]:
        """ Concatenating path with partition name with date partitions
        :param date_partitions      The date partitions
        :param path_name            The path with partition name
        :return concatenated list
        """
        return [path_name + process_date for process_date in date_partitions]

    def __add_partition_name(self, path_name: str) -> str:
        """ Adding partition name to path
        :param path_name         The path name
        :return path with partition name
        """
        if self.slash == path_name[-1]:
            path_name = path_name[:-1]
        return path_name + self.slash + self.partition_name + "="

    def __get_paths_with_process_name(self, path_name: str, **kwargs) -> List[str]:
        """ Getting paths with partition name
        :param path_name         The path name
        :return paths with partition name
        """
        date_partitions = self.get_date_partitions(path_name, **kwargs)
        if date_partitions:
            path_with_partition_name = self.__add_partition_name(path_name)
            return self.__concat_path_with_partition_name(date_partitions, path_with_partition_name)
        else:
            return []

    def read_dataframe(self, path_name: str = None, paths: List[str] = None, options=None) -> DataFrame:
        """ Reading multiple paths or only one path
        :param options           The options for spark
        :param paths             The path list
        :param path_name         The path name
        :return dataFrame
        """

        if options is None:
            options = {}

        path = self.__extract_path(path_name, paths)
        type_format = self.__get_format_type(path)

        if path_name is not None:
            paths = [path_name]
        return self.spark.read.format(type_format).load(paths, **options)

    def read_dataframes(self, path_name: str = None, paths: list = None, options=None, **kwargs) -> DataFrame:
        """ Reading multiple paths or only one path
        :param options           The options for spark
        :param paths             The path list
        :param path_name         The path name
        :return dataFrame
        """
        if options is None:
            options = {}

        if kwargs:
            paths = self.__get_paths_with_process_name(path_name, **kwargs)
            path_name = None
            if not paths:
                raise Exception("it wasn't found with the condition")

        if path_name is not None:
            paths = [path_name]

        return self.read_dataframe(paths=paths, options=options)

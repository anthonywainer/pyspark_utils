from datetime import datetime
from typing import List, Iterator
import re
import operator


class HDFSUtils:
    """Class that is used to connect to HDFS and get configurations list
    HDFS - Hadoop Distributed File System
    Attributes:
        :param partition_name   The partition name
        :param date_format      The date format
    """

    @property
    def date_regex(self):
        return r"(\d{1,4}([-])\d{1,2}([-])\d{1,4})|(\d{8,8})"

    @property
    def operation(self):
        return {
            "<": "operator.lt",
            "<=": "operator.le",
            "==": "operator.eq",
            "!=": "operator.ne",
            ">=": "operator.ge",
            ">": "operator.gt"
        }

    def __init__(self, sc, partition_name: str = "cutoff_date", date_format: str = '%Y-%m-%d'):
        __hadoop_config = sc._jsc.hadoopConfiguration()
        self.__file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(__hadoop_config)
        self.__Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        self.partition_name = partition_name
        self.date_format = date_format

    def __to_date(self, date_string: str) -> datetime:
        """ Converting string to date
        :param  date_string date in string format
        :return date formatted
        """
        return datetime.strptime(date_string, self.date_format)

    def __get_jvm_content(self, path_name: str):
        """ Getting content from HDFS in JVM object
        :param  path_name    The path name
        :return content list found
        """

        return self.__file_system.listStatus(self.__Path(path_name))

    def get_content(self, path_name: str) -> Iterator[str]:
        """ Getting files converted to string
        :param  path_name    The path name
        :return file list found
        """
        content = self.__get_jvm_content(path_name)

        return self.__to_string_jvm(content)

    def get_files(self, path_name: str) -> Iterator[str]:
        """ Getting files with extension
        :param  path_name    The path name
        :return files with extension
        """
        content = self.__get_jvm_content(path_name)

        files = filter(lambda file: file.isFile() and file.getLen() > 0, content)

        return self.__to_string_jvm(files)

    @staticmethod
    def __to_string_jvm(files) -> Iterator[str]:
        """ Converting string files from HDFS
        :param files    The file got from HDFS
        :return folder
        """

        return map(lambda file: file.getPath().toString(), files)

    def get_folders(self, path_name: str) -> Iterator[str]:
        """ Getting folders from HDFS
        :param  path_name    The path name
        :return folder list found
        """
        files = self.__get_jvm_content(path_name)
        folders = filter(lambda file: file.isDirectory(), files)

        return self.__to_string_jvm(folders)

    def __get_date(self, path_name: str) -> str:
        """ Getting date using regex
          :param path_name    The path name
          :return date in string
        """
        date = re.search(self.date_regex, path_name)

        if date:
            return date.group()

    def __filter_dates(self, files) -> Iterator[str]:
        """ filtering date partition in files
        :param files    All files in a path
        :return a list containing only the dates
        """
        date_files = filter(lambda path: self.partition_name in path, files)

        return map(lambda file: self.__get_date(file), date_files)

    def __sort_date_partitions(self, path_name: str, in_reverse: bool) -> List[datetime]:
        """ Sorting date partitions from HDFS
        :param path_name    The path name
        :return date partitions sorted descending
        """
        folders = self.get_folders(path_name)
        string_dates = self.__filter_dates(folders)

        dates = map(lambda file: self.__to_date(file), string_dates)

        return sorted(dates, reverse=in_reverse)

    def __format_date_partitions(self, date_partitions: Iterator[datetime]) -> List[str]:
        """ Formatting date partitions from process date
        :param date_partitions      The date partitions
        :return date partitions given date format
        """
        return list(map(lambda date: date.strftime(self.date_format), date_partitions))

    def __format_process_date(self, process_date: object) -> (datetime, datetime):
        """ Formatting process date
        :return two dates if is a list or a date if is string, in any case, thrown Exception
        """

        if isinstance(process_date, str):
            return self.__to_date(process_date), None

        if isinstance(process_date, list) and len(process_date) == 2:
            return self.__to_date(process_date[0]), self.__to_date(process_date[1])
        else:
            raise Exception("Process date incorrect")

    def __filter_date_partitions(self, date_partitions: List[datetime],
                                 process_date: object, operation: str) -> List[str]:
        """ Filtering date partitions from process date
        :param date_partitions      The date partitions
        :return date partitions filtered by process date<
        """
        range_condition = "begin_date <= date <= end_date"
        date_condition = "{}(date, begin_date)".format(self.operation[operation])

        begin_date, end_date = self.__format_process_date(process_date)
        condition = range_condition if end_date is not None else date_condition

        filtered_date_partitions = filter(
            lambda date: eval(condition,
                              {"date": date, "begin_date": begin_date,
                               "end_date": end_date, "operator": operator}),
            date_partitions
        )

        return list(self.__format_date_partitions(filtered_date_partitions))

    def get_date_partitions(self, path_name: str, process_date: object = None, operation: str = "<=",
                            partition_number: int = None, in_reverse: bool = True) -> List[str]:
        """ Getting date partitions from HDFS
        :param in_reverse        The list order
        :param operation         The operation to realize
        :param path_name         The path name
        :param process_date      The process date
        :param partition_number  The partition number
        :return date partitions
        """
        date_partitions = self.__sort_date_partitions(path_name, in_reverse)

        if process_date is not None:
            return self.__filter_date_partitions(date_partitions, process_date, operation)[: partition_number]

        return self.__format_date_partitions(date_partitions)[: partition_number]

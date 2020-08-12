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

    def get_content(self, path_name: str):
        """ Getting files from HDFS
        :param  path_name    The path name
        :return file list found
        """
        return self.__file_system.listStatus(self.__Path(path_name))

    @staticmethod
    def _filter_files(files) -> List[str]:
        """ Getting files with extensions
        :param files    The files got from HDFS
        :return file list with extensions
        """
        return [file.getPath().toString() for file in files if file.isFile() and file.getLen() > 0]

    def get_folders(self, path_name: str):
        """ Getting folders from HDFS
        :param  path_name    The path name
        :return folder list found
        """
        files = self.get_content(path_name)
        return [file.getPath().toString() for file in files if file.isDirectory()]

    def __find_date(self, path_name: str) -> str:
        """ Finding date using regex
          :param path_name    The path name
          :return date in string
        """
        data = re.compile(self.date_regex).findall(path_name)[0]
        filtered_date = [date for date in data if len(date) >= 8]
        return filtered_date[0] if filtered_date else None

    def __get_date(self, path: str) -> str:
        """ Getting date from file name
          :param path    The path name
          :return date in string if partition name is into path
        """
        return self.__find_date(path) if self.partition_name in path else False

    def __filter_dates(self, files) -> List[datetime]:
        """ filtering date partition in files
        :param files    All files in a path
        :return a list containing only the dates
        """
        return [self.__to_date(self.__get_date(file)) for file in files if self.__get_date(file)]

    def __sort_date_partitions(self, path_name: str) -> List[datetime]:
        """ Sorting date partitions from HDFS
        :param path_name    The path name
        :return date partitions sorted descending
        """
        files = self.get_folders(path_name)
        filtered_dates = self.__filter_dates(files)
        return sorted(filtered_dates, reverse=True)

    def __format_date_partitions(self, date_partitions: Iterator[datetime]) -> List[str]:
        """ Formatting date partitions from process date
        :param date_partitions      The date partitions
        :return date partitions given date format
        """
        return [date.strftime(self.date_format) for date in date_partitions]

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
        :return date partitions filtered by process date
        """

        global begin_date, end_date

        begin_date, end_date = self.__format_process_date(process_date)
        sentence = "begin_date <= date <= end_date" if end_date is not None else "{}({}, date)".format(
            self.operation[operation], "begin_date")

        filtered_date_partitions = filter(lambda date: eval(sentence), date_partitions)

        return self.__format_date_partitions(filtered_date_partitions)

    def get_date_partitions(self, path_name: str, process_date: object = None, operation: str = ">=",
                            partition_number: int = None) -> List[str]:
        """ Getting date partitions from HDFS
        :param operation         The operation to realize
        :param path_name         The path name
        :param process_date      The process date
        :param partition_number  The partition number
        :return date partitions
        """
        date_partitions = self.__sort_date_partitions(path_name)

        if process_date is not None:
            return self.__filter_date_partitions(date_partitions, process_date, operation)[: partition_number]

        return self.__format_date_partitions(date_partitions)[: partition_number]

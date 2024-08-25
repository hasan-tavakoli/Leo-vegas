from spark_utils import SparkUtils
from pyspark.sql import DataFrame


class DataProvider:
    def __init__(
        self,
        spark_utils: SparkUtils,
    ):

        self.spark_utils = spark_utils

    def extract_data(
        self, file_name: str, file_type: str = "csv", delimiter: str = ","
    ) -> DataFrame:
        """
        Extracts data from a specified file.

        :param file_name: Name of the file to extract data from
        :param file_type: Type of the file (default is 'csv')
        :param delimiter: Delimiter used in the file (default is ',')
        :return: DataFrame containing the extracted data
        """
        file_path = f"data/{file_name}.{file_type}"
        return self.spark_utils.read_csv(file_path, delimiter=delimiter)

    def load_data(
        self,
        df: DataFrame,
        output_name: str,
        file_type: str = "csv",
        delimiter: str = ",",
    ):
        """
        Saves the DataFrame to a specified file.

        :param df: DataFrame containing the data to save
        :param output_name: Name of the output file
        :param file_type: Type of the file to save (default is 'csv')
        :param delimiter: Delimiter to use in the output file (default is ',')
        """
        output_path = f"output/{output_name}.{file_type}"
        self.spark_utils.write_csv(df, output_path, delimiter=delimiter)

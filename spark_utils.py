import logging
from pyspark.sql import SparkSession
import pandas as pd


class SparkUtils:
    def __init__(self, app_name="SparkApp", master="local[*]"):
        """
        Initialize the Spark session.

        :param app_name: Name of the Spark application (default is 'SparkApp')
        :param master: The master URL for the Spark cluster (default is 'local[*]')
        """

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(app_name)

        self.spark_session = (
            SparkSession.builder.master(master).appName(app_name).getOrCreate()
        )
        logging.info("Spark session initialized successfully.")

    def read_csv(self, file_path, delimiter=",", infer_schema=True, header=True):
        """
        Reads a CSV file with a specified delimiter.

        :param file_path: Path to the CSV file
        :param delimiter: Delimiter used in the CSV file (default is ',')
        :param infer_schema: Whether to infer the schema of the CSV file (default is True)
        :param header: Whether the CSV file has a header row (default is True)
        :return: DataFrame containing the CSV data
        """
        try:
            df = (
                self.spark_session.read.option("header", header)
                .option("delimiter", delimiter)
                .option("inferSchema", infer_schema)
                .csv(file_path)
            )
            logging.info(f"CSV file {file_path} read successfully.")
            return df
        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {e}")

    def write_csv(self, df, output_path, delimiter=",", mode="overwrite", header=True):
        """
        Writes a DataFrame to a CSV file with the specified delimiter.

        :param df: DataFrame to write
        :param output_path: Path where the CSV file should be saved
        :param delimiter: Delimiter to use in the output CSV file (default is ',')
        :param mode: Save mode for the CSV file (default is 'overwrite')
        :param header: Whether to include a header row in the output CSV (default is True)
        """
        try:
            # df.repartition(1).write.option("header", header).option("delimiter", delimiter).mode(mode).csv(output_path)
            collected_data = df.collect()
            pdf = pd.DataFrame(collected_data, columns=df.columns)
            pdf.to_csv(output_path, sep=delimiter, index=False, header=header)
            logging.info(f"DataFrame written to {output_path} successfully.")
        except Exception as e:
            logging.error(f"Error writing DataFrame to {output_path}: {e}")

    def get_spark_session(self):
        """
        Returns the current Spark session.

        :return: SparkSession object
        """
        return self.spark_session

    def stop_spark_session(self):
        """
        Stops the current Spark session.
        """
        self.spark_session.stop()
        logging.info("Spark session stopped.")

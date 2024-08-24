from spark_utils import SparkUtils
from pyspark.sql import DataFrame


class DataProvider:
    def __init__(
        self,
        spark_utils: SparkUtils,
    ):

        self.spark_utils = spark_utils

    def extract_game_data(self) -> DataFrame:
        """
        Extracts the game data from the Game.csv file.

        :return: DataFrame containing the game data
        """
        file_path = "data/Game.csv"
        return self.spark_utils.read_csv(file_path, delimiter=",")

    def extract_game_category_data(self) -> DataFrame:
        """
        Extracts the game category data from the GameCategory.csv file.

        :return: DataFrame containing the game category data
        """
        file_path = "data/GameCategory.csv"
        return self.spark_utils.read_csv(file_path, delimiter=",")

    def extract_game_provider_data(self) -> DataFrame:
        """
        Extracts the game provider data from the GameProvider.csv file.

        :return: DataFrame containing the game provider data
        """
        file_path = "data/GameProvider.csv"
        return self.spark_utils.read_csv(file_path, delimiter=",")

    def load_dim_game_data(self, df: DataFrame):
        """
        Saves the DimGame data to a CSV file.

        :param df: DataFrame containing the DimGame data
        """
        output_path = "output/Dim_game.csv"
        self.spark_utils.write_csv(df, output_path)

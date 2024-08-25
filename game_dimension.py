from data_provider import DataProvider
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DimGame:
    def __init__(self, data_provider: DataProvider):

        self.data_provider = data_provider

    def run(self):
        game_df = self.data_provider.extract_data("Game")
        game_category_df = self.data_provider.extract_data("GameCategory")
        game_provider_df = self.data_provider.extract_data("GameProvider")

        transformed_df = self._transform_data(
            game_df, game_category_df, game_provider_df
        )

        self.data_provider.load_data(transformed_df, "Dim_game")

    def _transform_data(
        self,
        game_df: DataFrame,
        game_category_df: DataFrame,
        game_provider_df: DataFrame,
    ) -> DataFrame:
        """
        Transforms the raw game data into the DimGame dimensional model.

        :param game_df: DataFrame containing game data
        :param game_category_df: DataFrame containing game category data
        :param game_provider_df: DataFrame containing game provider data
        :return: Transformed DataFrame for DimGame
        """

        game_df = (
            game_df.withColumnRenamed("Game Name", "game_name")
            .withColumnRenamed("ID", "game_id")
            .withColumnRenamed("GameProviderId", "provider_id")
        )

        game_category_df = game_category_df.withColumnRenamed(
            "Game Category", "game_category"
        ).withColumnRenamed("Game ID", "game_id")

        game_provider_df = game_provider_df.withColumnRenamed(
            "ID", "provider_id"
        ).withColumnRenamed("Game Provider Name", "Provider_name")

        dim_game_df = (
        game_df.alias("game_df").join(
            game_category_df.alias("game_category_df"),
            col("game_df.game_id") == col("game_category_df.game_id"),
            how="left"
        )
        .join(
            game_provider_df.alias("game_provider_df"),
            col("game_df.provider_id") == col("game_provider_df.provider_id"),
            how="left"
        )
        .select(
            col("game_df.game_id"),
            col("game_df.game_name"),
            col("game_category_df.game_category"),
            col("game_provider_df.Provider_name")
        )
    )

        return dim_game_df

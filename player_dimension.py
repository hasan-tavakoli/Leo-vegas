from data_provider import DataProvider
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col


class PlayerGame:
    def __init__(self, data_provider: DataProvider):

        self.data_provider = data_provider

    def run(self):

        player_df = self.data_provider.extract_player_data()

        transformed_df = self._transform_data(player_df)

        self.data_provider.load_dim_player_data(transformed_df)

    def _transform_data(
        self,
        player_df: DataFrame,
    ) -> DataFrame:
        """
        Transforms the raw palyer data into the DimPlayer dimensional model.

        :param player_df: DataFrame containing game data
        :return: Transformed DataFrame for DimPlayer
        """

        window_spec = Window.partitionBy("playerID").orderBy(col("latestUpdate").desc())
        dim_player_df = (
            player_df.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .select(
                col("playerID").alias("player_id"),
                col("gender"),
                col("country"),
                col("latestUpdate"),
            )
        )

        return dim_player_df

from data_provider import DataProvider
from pyspark.sql import DataFrame


class FactBet:
    def __init__(self, data_provider: DataProvider):

        self.data_provider = data_provider

    def run(self):

        transaction_df = self.data_provider.extract_game_transaction_data()

        transformed_df = self._transform_data(transaction_df)

    def _transform_data(
        self,
        player_df: DataFrame,
    ) -> DataFrame:
        """
        Transforms raw data into the FactBet fact table.

        :param transaction_df: DataFrame containing transaction data
        :return: Transformed DataFrame for FactBet
        """

        return dim_player_df

from pyspark.sql import DataFrame
from data_provider import DataProvider


class CachedData:
    def __init__(self, data_provider: DataProvider):
        self.data_provider = data_provider
        self._cached_player_df = None

    def get_player_df(self) -> DataFrame:
        if self._cached_player_df is None:
            self._cached_player_df = self.data_provider.extract_data("Player").cache()
        return self._cached_player_df

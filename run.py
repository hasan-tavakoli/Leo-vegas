from datetime import datetime
from spark_utils import SparkUtils
from data_provider import DataProvider
from game_dimension import DimGame
from player_dimension import PlayerGame
from fact_bet import FactBet
from cached_data import CachedData


class AnalyticsPipeline:
    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
    ):

        self.start_date = start_date
        self.end_date = end_date

        self.spark_utils = SparkUtils(app_name="HTSparkApp")

        self.data_provider = DataProvider(
            self.spark_utils,
        )
        self.cache_data = CachedData(self.data_provider)
        self.dim_game = DimGame(self.data_provider)
        self.dim_player = PlayerGame(self.data_provider, self.cache_data)
        self.fact_bet = FactBet(
            self.data_provider, self.cache_data, self.start_date, self.end_date
        )

    def run(self):

        self.dim_game.run()
        self.dim_player.run()
        self.fact_bet.run()


if __name__ == "__main__":

    start_date = datetime.strptime("2017-03-19", "%Y-%m-%d")
    end_date = datetime.strptime("2017-04-08", "%Y-%m-%d")
    pipeline = AnalyticsPipeline(start_date, end_date)
    pipeline.run()

from data_provider import DataProvider
from datetime import datetime
from pyspark.sql import DataFrame
from cached_data import CachedData
from pyspark.sql.functions import col, when, first, sum, broadcast
from pyspark.sql.window import Window


class FactBet:
    def __init__(
        self,
        data_provider: DataProvider,
        cache_data: CachedData,
        start_date: datetime,
        end_date: datetime,
    ):

        self.data_provider = data_provider
        self.player_df = cache_data.get_player_df()
        self.start_date = start_date
        self.end_date = end_date

    def run(self):

        transaction_df = self.data_provider.extract_data(
            file_name="GameTransaction", delimiter=";"
        )
        currency_exchange_df = self.data_provider.extract_data("CurrencyExchange")
        broadcast_currency_exchange_df = broadcast(currency_exchange_df)
        if self.player_df is None:
            raise ValueError(
                "Player data is not cached. Please ensure PlayerGame.run() is called first."
            )

        broadcast_player_df = broadcast(self.player_df)

        transformed_df = self._transform_data(
            transaction_df, broadcast_currency_exchange_df, broadcast_player_df
        )
        self.data_provider.load_data(transformed_df, "Fact_bet")

    def _transform_data(
        self,
        transaction_df: DataFrame,
        currency_exchange_df: DataFrame,
        player_df: DataFrame,
    ) -> DataFrame:
        """
        Transforms raw data into the FactBet fact table.

        :param transaction_df: DataFrame containing transaction data
        :param currency_exchange_df: DataFrame containing currency exchange data
        :param player_df: DataFrame containing player data
        :return: Transformed DataFrame for FactBet
        """

        transaction_df = transaction_df.filter(
            (col("date") > self.start_date) & (col("date") <= self.end_date)
        )

        game_transaction_currency_df = (
            transaction_df.alias("t")
            .join(
                currency_exchange_df.alias("c"),
                (col("t.txCurrency") == col("c.currency"))
                & (col("t.date") == col("c.Date")),
                how="inner",
            )
            .select(
                col("t.date"),
                col("t.PlayerId"),
                col("t.gameID"),
                when(
                    col("t.txType") == "WAGER",
                    col("t.realAmount") * col("c.baseRateEuro"),
                )
                .otherwise(0)
                .alias("Cash_turnover"),
                when(
                    col("t.txType") == "WAGER",
                    col("t.bonusAmount") * col("c.baseRateEuro"),
                )
                .otherwise(0)
                .alias("Bonus_turnover"),
                when(
                    col("t.txType") == "RESULT",
                    col("t.realAmount") * col("c.baseRateEuro"),
                )
                .otherwise(0)
                .alias("Cash_winnings"),
                when(
                    col("t.txType") == "RESULT",
                    col("t.bonusAmount") * col("c.baseRateEuro"),
                )
                .otherwise(0)
                .alias("Bonus_winnings"),
            )
        )
        window_spec = Window.partitionBy("t.PlayerId", "t.date").orderBy(
            col("p.latestUpdate").desc()
        )
        player_df = player_df.alias("p")
        player_df = (
            transaction_df.alias("t")
            .join(
                player_df,
                (col("p.PlayerId") == col("t.PlayerId"))
                & (col("p.latestUpdate") <= col("t.date")),
                how="left",
            )
            .withColumn("country", first(col("p.country")).over(window_spec))
            .select(col("t.date"), col("t.PlayerId"), col("t.gameID"), col("country"))
            .distinct()
        )

        game_transaction_player_country_df = (
            game_transaction_currency_df.alias("gtc")
            .join(
                player_df.alias("p"),
                (col("gtc.date") == col("p.date"))
                & (col("gtc.PlayerId") == col("p.PlayerId"))
                & (col("gtc.gameID") == col("p.gameID")),
                how="inner",
            )
            .select(
                col("gtc.date"),
                col("gtc.PlayerId"),
                col("p.country"),
                col("gtc.gameID"),
                col("gtc.Cash_turnover"),
                col("gtc.Bonus_turnover"),
                col("gtc.Cash_winnings"),
                col("gtc.Bonus_winnings"),
                (col("gtc.Cash_turnover") + col("gtc.Bonus_turnover")).alias(
                    "Turnover"
                ),
                (col("gtc.Cash_winnings") + col("gtc.Bonus_winnings")).alias(
                    "Winnings"
                ),
                (col("gtc.Cash_turnover") - col("gtc.Cash_winnings")).alias(
                    "Cash_result"
                ),
                (col("gtc.Bonus_turnover") - col("gtc.Bonus_winnings")).alias(
                    "Bonus_result"
                ),
                (
                    (col("gtc.Cash_turnover") + col("gtc.Bonus_turnover"))
                    - (col("gtc.Cash_winnings") + col("gtc.Bonus_winnings"))
                ).alias("Gross_result"),
            )
        )

        final_df = (
            game_transaction_player_country_df.groupBy(
                col("date"),
                col("PlayerId").alias("player_id"),
                col("country"),
                col("gameID").alias("game_id"),
            )
            .agg(
                sum("Cash_turnover").alias("Cash turnover"),
                sum("Bonus_turnover").alias("Bonus turnover"),
                sum("Cash_winnings").alias("Cash winnings"),
                sum("Bonus_winnings").alias("Bonus winnings"),
                sum("Turnover").alias("Turnover"),
                sum("Winnings").alias("Winnings"),
                sum("Cash_result").alias("Cash result"),
                sum("Bonus_result").alias("Bonus result"),
                sum("Gross_result").alias("Gross result"),
            )
            .orderBy(col("date"), col("PlayerId"), col("country"), col("gameID"))
        )

        return final_df

import unittest
from unittest.mock import MagicMock
from player_dimension import PlayerGame
from spark_utils import SparkUtils


class TestPlayerGame(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up a SparkSession for the tests using SparkUtils.
        """
        cls.spark_utils = SparkUtils(app_name="TestPlayerGame", master="local[*]")
        cls.spark = cls.spark_utils.get_spark_session()

    @classmethod
    def tearDownClass(cls):
        """
        Stop the SparkSession after the tests using SparkUtils.
        """
        cls.spark_utils.stop_spark_session()

    def setUp(self):
        """
        Set up the PlayerGame instance with mocked CachedData.
        """
        self.data_provider = MagicMock()
        self.cache_data = MagicMock()

        player_data = [
            (1, "AT", "31900", "FEMALE", "VALIDATED", 0, 0, 0, "2017-01-01"),
            (1, "AT", "31900", "FEMALE", "VALIDATED", 0, 0, 0, "2017-03-01"),
            (2, "SE", "33701", "MALE", "SIGNUP", 0, 0, 0, "2017-01-01"),
            (3, "SE", "22335", "MALE", "VALIDATED", 1, 1, 1, "2017-01-01"),
            (3, "SE", "22335", "FEMALE", "VALIDATED", 1, 1, 1, "2017-03-01"),
            (4, "SE", "24782", "MALE", "SIGNUP", 1, 1, 1, "2017-01-01"),
            (5, "GB", "22990", "FEMALE", "VALIDATED", 1, 1, 1, "2017-01-01"),
            (5, "GB", "22990", "FEMALE", "VALIDATED", 1, 1, 1, "2017-02-01"),
            (6, "FI", "34604", "MALE", "VALIDATED", 0, 0, 0, "2017-01-01"),
            (6, "FI", "34604", "MALE", "VALIDATED", 0, 0, 0, "2017-01-15"),
            (7, "NO", "30732", "MALE", "SIGNUP", 1, 1, 1, "2017-01-01"),
            (8, "SE", "28571", "MALE", "VALIDATED", 0, 1, 1, "2017-01-01"),
            (8, "SE", "28571", "MALE", "VALIDATED", 0, 1, 1, "2017-02-01"),
            (9, "GB", "29193", "FEMALE", "SIGNUP", 1, 1, 1, "2017-01-01"),
            (10, "SE", "32679", "FEMALE", "INACTIVE", 0, 1, 1, "2017-01-01"),
        ]
        self.player_df = self.spark.createDataFrame(
            player_data,
            [
                "playerID",
                "country",
                "BirthDate",
                "gender",
                "playerState",
                "VIP",
                "KYC",
                "wantsNewsletter",
                "latestUpdate",
            ],
        )

        self.cache_data.get_player_df.return_value = self.player_df
        self.player_game = PlayerGame(self.data_provider, self.cache_data)

    def test_transform_data(self):
        """
        Test the _transform_data method to ensure it performs the transformation correctly.
        """

        result_df = self.player_game._transform_data(self.player_df)

        expected_data = [
            (1, "FEMALE", "AT", "2017-03-01"),
            (2, "MALE", "SE", "2017-01-01"),
            (3, "FEMALE", "SE", "2017-03-01"),
            (4, "MALE", "SE", "2017-01-01"),
            (5, "FEMALE", "GB", "2017-02-01"),
            (6, "MALE", "FI", "2017-01-15"),
            (7, "MALE", "NO", "2017-01-01"),
            (8, "MALE", "SE", "2017-02-01"),
            (9, "FEMALE", "GB", "2017-01-01"),
            (10, "FEMALE", "SE", "2017-01-01"),
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, ["player_id", "gender", "country", "latestUpdate"]
        )

        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()

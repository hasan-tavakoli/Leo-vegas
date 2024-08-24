
from datetime import datetime, timedelta
import logging

from spark_utils import SparkUtils
from data_provider import DataProvider
from game_dimension import DimGame


# logging.basicConfig(level="DEBUG")


class AnalyticsPipeline:
    
    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
    ):
        
        self.start_date = start_date
        self.end_date = end_date
        
        self.spark_utils = SparkUtils(app_name="MySparkApp")
    
        self.data_provider = DataProvider(
             self.spark_utils,
        )
        self.dim_game=DimGame(
            
            self.data_provider
        )
         
        
    def run(self):
        
        self.dim_game.run()
        
        
        


if __name__ == "__main__":
    current_timestamp = datetime.utcnow()
    start_date = current_timestamp - timedelta(days=3)
    end_date = current_timestamp - timedelta(days=2)
    pipeline = AnalyticsPipeline(
       start_date, end_date
    )
    pipeline.run()
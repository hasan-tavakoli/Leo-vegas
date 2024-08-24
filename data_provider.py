from spark_utils import SparkUtils


class DataProvider:
    def __init__(
        self,
        spark_utils: SparkUtils,
    ):
        
        self.spark_utils = spark_utils
        print(self.spark_utils)
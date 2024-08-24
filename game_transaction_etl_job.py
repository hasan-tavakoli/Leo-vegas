from pyspark.sql import SparkSession


class GameTransactionETLJob:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("GameTransactionETLJob")
                                          .getOrCreate())

    def extract(self):
        print(self.spark_session)

    def transform(self, df):
        print("transform")
        
    def load(self, df):
        print("load")
        
    def run(self):
        self.extract()

if __name__ == "__main__":
    etl_job = GameTransactionETLJob()
    etl_job.run()

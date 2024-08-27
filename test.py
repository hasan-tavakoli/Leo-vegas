from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

columns = ["Seqno", "Name"]
data = [("1", "1,3"), ("2", "tracey smith"), ("3", "amy sanders")]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)


def _convert_to_float(value: str) -> float:
    """
    Convert a string with comma as decimal separator to a float.

    :param value: The string value to convert.
    :return: The converted float value.
    """
    try:
        if value:
            return float(value.replace(",", "."))
        return None
    except ValueError:
        return None


def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1 : len(x)] + " "
    return resStr


convertUDF = udf(lambda z: _convert_to_float(z), StringType())

df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)

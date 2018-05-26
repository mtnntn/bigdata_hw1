import os
import sys
import subprocess
import pyspark as ps
from pyspark.sql import SQLContext

"""
 Il Job genera, per ciascun anno, le dieci parole che sono state piu' usate nelle recensioni (campo 'Summary'). 
 L'output e' ordinato, per ogni anno, sulla frequenza delle parole. 
 Per frequenza si intende il numero di occorrenze della parola nelle recensioni di quell'anno.
"""


""" Defining Constants """
master = "local[*]"
app_name = "Esercizio2"
input_filename = "reviews_clean.csv"  # "test_shuffle_clean.csv"  #
input_path = os.path.join(os.sep, "user", "root", input_filename)
output_folder_name = "23"
output_path = os.path.join(os.sep, "user", "root", "output_spark", output_folder_name)

csv_format = "com.databricks.spark.csv"

""" Job > Constants """

header = {
    "Id": 0,
    "ProductId": 1,
    "UserId": 2,
    "ProfileName": 3,
    "HelpfulnessNumerator": 4,
    "HelpfulnessDenominator": 5,
    "Score": 6,
    "Time": 7,
    "Summary": 8,
    "Text": 9
}

""" Get Input """
# Expected arguments-> "path/to/input", "path/to/output"
arguments = sys.argv
arguments.pop(0)
if len(arguments) == 2:
    input_path = arguments[0]
    output_path = arguments[1]

""" Cleaning Output Folder """

rmdir_cmd = ["hdfs", "dfs", "-rm", "-r", output_path]
proc = subprocess.Popen(rmdir_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
proc.communicate()

""" Job > Main """

conf = ps.SparkConf().setMaster(master).setAppName(app_name)
spark_context = ps.SparkContext(conf=conf)
sqlContext = SQLContext(spark_context)

df = sqlContext.read.format(csv_format).option("inferSchema", "true").option("header", "true")

df = df.load(input_path)
df = df.withColumn("Time", df['Time'].substr(0, 4))
df.registerTempTable("reviews")

sql = "SELECT ProductId as product, reviews.Time as year, AVG(Score) as mean_score " \
      "FROM reviews " \
      "WHERE reviews.Time >= 2003 AND reviews.Time <= 2011 " \
      "GROUP BY ProductId, reviews.Time " \
      "ORDER BY ProductId ASC, reviews.Time ASC"

df2 = sqlContext.sql(sql).coalesce(1).write.format(csv_format).save(output_path)

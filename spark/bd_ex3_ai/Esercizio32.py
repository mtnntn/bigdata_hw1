import os
import sys
import subprocess
import pyspark as ps

from pyspark.sql import SQLContext

"""
 Il Job genera coppie di prodotti che hanno almeno un utente in comune.
 Un utente e' comune a due prodotti se ha recensito entrambi questi prodotti.
 In output viene indicato, per ciascuna coppia di prodotti, il numero di utenti comune. 
 Il risultato e' ordinato in base al ProductId del primo elemento della coppia e non sono presenti duplicati. 
"""


""" Defining Constants """
master = "local[*]"
app_name = "Esercizio3"
input_filename = "reviews_clean.csv"  # "test_shuffle_clean.csv"  #
input_path = os.path.join(os.sep, "user", "root", input_filename)
output_folder_name = "33"
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

df = df.load(input_path).registerTempTable("reviews")

sql_1 = "SELECT r1.ProductId as p1, r2.ProductId as p2, SUM(1) as common_reviewers " \
        "FROM reviews r1 JOIN reviews r2 " \
        "WHERE r1.UserId = r2.UserId AND r1.ProductId < r2.ProductId " \
        "GROUP BY r1.ProductId, r2.ProductId " \
        "ORDER BY r1.ProductId ASC, r2.ProductId ASC"

df2 = sqlContext.sql(sql_1).coalesce(1).write.format(csv_format).save(output_path)


# spark-submit --packages com.databricks:spark-csv_2.10:1.2.0

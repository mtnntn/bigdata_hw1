import os
import sys
import subprocess
import pyspark as ps

"""
 Questo job genera, per ciascun prodotto, lo score medio ottenuto in ogni anno compreso tra il 2003 e il 2012.
 Il formato in output e' ProductId - Anno - Score medio ottenuto nell' anno. 
"""


""" Defining Constants """
master = "local[*]"
app_name = "Esercizio2"
input_filename = "reviews_clean.csv"  # "test_shuffle_clean.csv"
input_path = os.path.join(os.sep, "user", "root", input_filename)
output_folder_name = "2"
output_path = os.path.join(os.sep, "user", "root", "output_spark", output_folder_name)

""" Job > Constants """

min_year = 2003
max_year = 2012

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

if len(arguments) == 4:
    min_year = arguments[2]
    max_year = arguments[3]

    if int(min_year) > int(max_year):
        min_year = arguments[3]
        max_year = arguments[2]

""" Cleaning Output Folder """

rmdir_cmd = ["hdfs", "dfs", "-rm", "-r", output_path]
proc = subprocess.Popen(rmdir_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
proc.communicate()


""" Job > Functions """


def filter_review_by_date(x):
    res = False
    try:
        year = int(x.split(",")[header["Time"]].split("-")[0])
        res = min_year <= year <= max_year
    except Exception as e:
        print("Error occurred while processing line %s " % x, e)
    return res


def get_key_value_tuple(x):
    splitted_line = x.split(",")
    product = splitted_line[header["ProductId"]]
    year = splitted_line[header["Time"]].split("-")[0]
    score = splitted_line[header["Score"]]
    return product+"-"+year, score


def get_key_value_avg(x):
    den = len(x)
    num = 0.0
    for i in x:
        try:
            num += int(i)
        except Exception as e:
            print("Exception occurred while calculating average ", e)
            return 0
    return num/den


def get_output_from_key_value(x):
    splitted_key = x[0].split("-")
    product = splitted_key[0]
    year = splitted_key[1]
    avg_score = x[1]
    return str(product) + " " + str(year) + " " + str(avg_score)


""" Job > Main """

conf = ps.SparkConf().setMaster(master).setAppName(app_name)

spark_context = ps.SparkContext(conf=conf)

lines = spark_context.textFile(input_path)

lines_filtered = lines.filter(filter_review_by_date)

year_product_vote = lines_filtered.map(get_key_value_tuple)

year_product_votes = year_product_vote.groupByKey()

year_product_mean = year_product_votes.mapValues(get_key_value_avg)

output = year_product_mean.map(get_output_from_key_value).sortBy(lambda x: (x.split(" ")[0], x.split(" ")[1]))

output.saveAsTextFile(output_path)

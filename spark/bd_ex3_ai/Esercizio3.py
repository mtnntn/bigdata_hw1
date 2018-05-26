import os
import sys
import subprocess
import pyspark as ps


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
output_folder_name = "3"
output_path = os.path.join(os.sep, "user", "root", "output_spark", output_folder_name)

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

""" Job > Functions """


def get_user_products_tuple(row):
    row = row.split(",")
    return row[header["UserId"]], row[header["ProductId"]]


def get_product_couples_keys(user_prods):
    products = list(user_prods[1])

    couples = set()

    while len(products) > 1:
        current = products.pop()
        for prod2 in products:

            k1 = current
            k2 = prod2

            if k1 > k2:
                k1 = prod2
                k2 = current

            k = k1+"-"+k2
            couples.add((k, 1))

    return couples


def same_product(couple):
    prod1, prod2 = couple[0].split("-")
    return prod1 != prod2


def get_output_string(tpl):
    p1, p2 = tpl[0].split("-")
    val = tpl[1]
    return p1 + " " + p2 + " " + str(val)


""" Job > Main """

conf = ps.SparkConf().setMaster(master).setAppName(app_name)

spark_context = ps.SparkContext(conf=conf)

loaded_file = spark_context.textFile(input_path)

user_products = loaded_file.map(get_user_products_tuple).groupByKey().filter(lambda x: len(list(x[1])) >= 2)

product_couples = user_products.flatMap(get_product_couples_keys).reduceByKey(lambda v1, v2: v1+v2)

output = product_couples.map(get_output_string).sortBy(lambda x: (x.split(" ")[0], x.split(" ")[1], x.split(" ")[2]))

output.saveAsTextFile(output_path)

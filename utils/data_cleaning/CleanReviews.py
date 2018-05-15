import pandas as pd
import os
import string
import datetime as dt

workdir_path = os.path.join("datasets")
input_filename = "Reviews.csv"
input_file_path = os.path.join(workdir_path, input_filename)

output_filename = "reviews_clean.csv"
output_file_path = os.path.join(workdir_path, output_filename)

fmt = '%Y-%m-%d %H:%M:%S'

# Id ProductId UserId ProfileName HelpfulnessNumerator HelpfulnessDenominator Score Time Summary Text

def clean_text(text_to_clean):
    exclude = set(string.punctuation)
    result = text_to_clean
    try:
        result = ''.join(ch for ch in text_to_clean if ch not in exclude).lower()
    except Exception as e:
        print("Row skipped->", text_to_clean, e)
    return result


begin = dt.datetime.now()
print("Start Cleaning -> %s " % begin)
reviews = pd.read_csv(input_file_path)
reviews["Time"] = pd.to_datetime(reviews["Time"], unit="s")
reviews["ProfileName"] = reviews["ProfileName"].apply(clean_text)
reviews["Summary"] = reviews["Summary"].apply(clean_text)
reviews["Text"] = reviews["Text"].apply(clean_text)
reviews.to_csv(output_file_path, index=False, sep=",", line_terminator="\n")
end = dt.datetime.now()
seconds = (end - begin).total_seconds()
print("Cleaning complete -> %s -> %s seconds" % (str(end), seconds))


# 568454
# Start Cleaning -> 2018-05-15 01:02:12.020251
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Row skipped-> nan 'float' object is not iterable
# Cleaning complete -> 2018-05-15 01:03:01.766965 -> 0.8291118999999999 minutes

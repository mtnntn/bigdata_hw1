CREATE DATABASE amazon_reviews WITH DBPROPERTIES ("creator" = "Antonio Matinata", "date" = "2018-05-10");

CREATE TABLE IF NOT EXISTS amazon_reviews.reviews (
        id INT COMMENT "Review unique id",
        productId STRING COMMENT "Unique identifier for the product",
        userId STRING COMMENT "Unique identifier for the user",
        profileName STRING COMMENT "Account name of the user",
        helpfulnessNumerator FLOAT COMMENT "Number of users who found the review helpful",
        helpfulnessDenominator FLOAT COMMENT"Number of users who graded the review",
        score INT COMMENT "Rating between 1 and 5",
        time DATE COMMENT "Date of the review expressed in the format YYYY-MM-DD",
        summary STRING COMMENT "Summary of the review",
        text STRING COMMENT "Text of the review"
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
        tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/root/reviews_clean.csv' OVERWRITE INTO TABLE amazon_reviews.reviews;

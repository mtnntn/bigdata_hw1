INSERT OVERWRITE DIRECTORY '/user/root/output_hive/0/2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ProductId as product, YEAR(Time) as year, AVG(Score) as mean_score
FROM amazon_reviews.reviews
WHERE YEAR(Time) >= 2003 AND YEAR(Time) <= 2011
GROUP BY ProductId, YEAR(Time)
ORDER BY ProductId ASC, year ASC;

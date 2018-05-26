INSERT OVERWRITE DIRECTORY '/user/root/output_hive/0/3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT r1.ProductId as p1, r2.ProductId as p2, SUM(1) as common_reviewers
FROM amazon_reviews.reviews r1 JOIN amazon_reviews.reviews r2
WHERE r1.UserId = r2.UserId AND r1.ProductId < r2.ProductId
GROUP BY r1.ProductId, r2.ProductId
ORDER BY r1.ProductId ASC, r2.ProductId ASC;


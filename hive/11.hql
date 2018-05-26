INSERT OVERWRITE DIRECTORY '/user/root/output_hive/0/11'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ranked.year, ranked.word, ranked.frequency
FROM (SELECT temp.year, temp.word, temp.frequency,  rank() over (PARTITION BY temp.year ORDER BY temp.frequency DESC) as rank
        FROM (  SELECT YEAR(Time) as year, t.t1 as word, count(*) as frequency
                FROM  amazon_reviews.reviews LATERAL VIEW EXPLODE(SPLIT(Summary, " ")) t as t1
                GROUP BY t.t1, YEAR(Time)) temp
                ) ranked
WHERE ranked.rank < 10
ORDER BY ranked.year ASC, ranked.frequency DESC, ranked.word ASC;

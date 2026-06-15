import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DoubanAmdahlSpark").getOrCreate()

start = time.perf_counter()

df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", "true")
    .csv("file:///opt/spark/work/data/movies.csv")
)

clean = (
    df.dropDuplicates(["movie_id"])
      .dropna(subset=["year", "rating_score"])
      .fillna({
          "genres": "未知类型",
          "countries": "未知国家",
          "directors": "未知导演",
          "summary": "暂无简介"
      })
)

clean.createOrReplaceTempView("movies")

sql = """
WITH pairs AS (
  SELECT movie_id, country, genre
  FROM movies
  LATERAL VIEW explode(split(countries, '/')) c AS country
  LATERAL VIEW explode(split(genres, '/')) g AS genre
),
country_genre AS (
  SELECT TRIM(country) AS country,
         TRIM(genre) AS genre,
         COUNT(DISTINCT movie_id) AS cnt
  FROM pairs
  WHERE TRIM(country) <> ''
    AND TRIM(genre) NOT IN ('', '????', '未知类型')
  GROUP BY TRIM(country), TRIM(genre)
),
country_total AS (
  SELECT country, SUM(cnt) AS country_cnt
  FROM country_genre
  GROUP BY country
),
global_genre AS (
  SELECT genre, SUM(cnt) AS genre_cnt
  FROM country_genre
  GROUP BY genre
),
global_total AS (
  SELECT SUM(cnt) AS total_cnt
  FROM country_genre
)
SELECT cg.country,
       cg.genre,
       cg.cnt,
       ROUND(cg.cnt / ct.country_cnt, 4) AS country_share,
       ROUND(gg.genre_cnt / gt.total_cnt, 4) AS global_share,
       ROUND((cg.cnt / ct.country_cnt) / (gg.genre_cnt / gt.total_cnt), 2) AS lift
FROM country_genre cg
JOIN country_total ct ON cg.country = ct.country
JOIN global_genre gg ON cg.genre = gg.genre
CROSS JOIN global_total gt
WHERE ct.country_cnt >= 500
  AND gg.genre_cnt >= 300
  AND cg.cnt >= 50
ORDER BY lift DESC
LIMIT 20
"""
spark.sql(sql).show(20, truncate=False)

elapsed = time.perf_counter() - start
print(f"耗时：{elapsed:.4f}s, executorInstances={spark.conf.get('spark.executor.instances', 'unknown')}")

spark.stop()

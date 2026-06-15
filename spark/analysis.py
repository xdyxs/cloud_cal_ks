from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, count, lit, explode, split

spark = SparkSession.builder.appName("DoubanMovieCleaning").getOrCreate()

input_path = "file:///opt/spark/work/data/movies.csv"

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", "true")
    .csv(input_path)
)

print("Schema:")
df.printSchema()

print("前5行:")
df.select("movie_id", "title", "year", "rating_score", "genres", "countries").show(5, truncate=False)

raw_count = df.count()

print("各字段缺失值比例:")
missing_rows = []
for c in df.columns:
    missing_count = df.filter(
        col(c).isNull() | (trim(col(c).cast("string")) == "")
    ).count()
    missing_rows.append((c, missing_count, missing_count / raw_count))

missing_df = spark.createDataFrame(
    missing_rows,
    ["字段", "缺失值数量", "缺失值比例"]
)

missing_df.show(truncate=False)

clean_df = df.dropDuplicates(["movie_id"])

clean_df = clean_df.dropna(subset=["year", "rating_score"])

clean_df = clean_df.fillna({
    "genres": "未知类型",
    "countries": "未知国家",
    "directors": "未知导演",
    "summary": "暂无简介"
})

clean_df = (
    clean_df
    .withColumn("year", col("year").cast("int"))
    .withColumn("rating_score", col("rating_score").cast("double"))
    .withColumn("rating_count", col("rating_count").cast("long"))
    .withColumn("collect_count", col("collect_count").cast("long"))
)

clean_count = clean_df.count()

print("清理前后的行数:")
print(f"清洗前: {raw_count}")
print(f"清洗后: {clean_count}")
print(f"移除行数: {raw_count - clean_count}")

print("数值字段统计:")
clean_df.select(
    "year",
    "rating_score",
    "rating_count",
    "collect_count"
).summary("mean", "stddev", "min", "max").show(truncate=False)

print("前10个类型:")
clean_df.groupBy("genres").count().orderBy(col("count").desc()).show(10, truncate=False)

print("前10个国家:")
clean_df.groupBy("countries").count().orderBy(col("count").desc()).show(10, truncate=False)

# ============================================================
# A-2: Spark SQL analysis
# ============================================================
clean_df.createOrReplaceTempView("movies")

# explode genres for analysis
genre_df = (
    clean_df.select("movie_id", "title", "year", "rating_score", "rating_count", "collect_count",
                     explode(split(col("genres"), "/")).alias("genre"))
         .withColumn("genre", trim(col("genre")))
         .filter(~col("genre").isin("", "未知类型", "????"))
)
genre_df.createOrReplaceTempView("movie_genres")

# explode directors for analysis
director_df = (
    clean_df.select("movie_id", "title", "year", "rating_score", "rating_count",
                     explode(split(col("directors"), "/")).alias("director"))
         .withColumn("director", trim(col("director")))
         .filter(~col("director").isin("", "未知导演"))
)
director_df.createOrReplaceTempView("movie_directors")

def show_sql(title, sql, n=20):
    print(f"\n--- {title} ---")
    spark.sql(sql).show(n, truncate=False)

show_sql("Q1_GROUP_BY_GENRE", """
SELECT genre,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_score), 2) AS avg_rating,
       ROUND(AVG(rating_count), 0) AS avg_rating_count
FROM movie_genres
GROUP BY genre
HAVING movie_count >= 100
ORDER BY avg_rating DESC
LIMIT 10
""")

show_sql("Q2_TOP_DIRECTORS", """
SELECT director,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_score), 2) AS avg_rating,
       SUM(rating_count) AS total_rating_count
FROM movie_directors
GROUP BY director
HAVING movie_count >= 5
ORDER BY avg_rating DESC, total_rating_count DESC
LIMIT 10
""")

show_sql("Q3_YEAR_TREND", """
SELECT year,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_score), 2) AS avg_rating,
       SUM(rating_count) AS total_rating_count
FROM movies
WHERE year >= 1980
GROUP BY year
ORDER BY year DESC
LIMIT 20
""")

show_sql("Q4_DECADE_GENRE_WINDOW", """
WITH genre_decade AS (
  SELECT FLOOR(year / 10) * 10 AS decade,
         genre,
         COUNT(*) AS movie_count,
         ROUND(AVG(rating_score), 2) AS avg_rating
  FROM movie_genres
  GROUP BY FLOOR(year / 10) * 10, genre
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY decade ORDER BY movie_count DESC) AS rn
  FROM genre_decade
)
SELECT decade, genre, movie_count, avg_rating, rn
FROM ranked
WHERE rn <= 3
ORDER BY decade DESC, rn
""", 30)

show_sql("Q5_WEIGHTED_RATING_TOP", """
WITH base AS (
  SELECT AVG(rating_score) AS c,
         percentile_approx(rating_count, 0.75) AS m
  FROM movies
  WHERE rating_count > 0
),
scored AS (
  SELECT title, year, rating_score, rating_count,
         ROUND(
           rating_count / (rating_count + m) * rating_score
           + m / (rating_count + m) * c,
           3
         ) AS weighted_score
  FROM movies CROSS JOIN base
  WHERE rating_count > 0
)
SELECT title, year, rating_score, rating_count, weighted_score
FROM scored
ORDER BY weighted_score DESC
LIMIT 15
""")

show_sql("Q6_COUNTRY_GENRE_LIFT", """
WITH pairs AS (
  SELECT movie_id, country, genre
  FROM movies
  LATERAL VIEW explode(split(countries, '/')) c AS country
  LATERAL VIEW explode(split(genres, '/')) g AS genre
),
country_genre AS (
  SELECT TRIM(country) AS country, TRIM(genre) AS genre, COUNT(DISTINCT movie_id) AS cnt
  FROM pairs
  WHERE TRIM(country) <> '' AND TRIM(genre) NOT IN ('', '????', '未知类型')
  GROUP BY TRIM(country), TRIM(genre)
),
country_total AS (
  SELECT country, SUM(cnt) AS country_cnt FROM country_genre GROUP BY country
),
global_genre AS (
  SELECT genre, SUM(cnt) AS genre_cnt FROM country_genre GROUP BY genre
),
global_total AS (
  SELECT SUM(cnt) AS total_cnt FROM country_genre
)
SELECT cg.country, cg.genre, cg.cnt,
       ROUND(cg.cnt / ct.country_cnt, 4) AS country_share,
       ROUND(gg.genre_cnt / gt.total_cnt, 4) AS global_share,
       ROUND((cg.cnt / ct.country_cnt) / (gg.genre_cnt / gt.total_cnt), 2) AS lift
FROM country_genre cg
JOIN country_total ct ON cg.country = ct.country
JOIN global_genre gg ON cg.genre = gg.genre
CROSS JOIN global_total gt
WHERE ct.country_cnt >= 500 AND gg.genre_cnt >= 300 AND cg.cnt >= 50
ORDER BY lift DESC
LIMIT 20
""")

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, stddev, min as F_min, max as F_max,
    year, month, to_date, split, explode, row_number, regexp_replace
)
from pyspark.sql.window import Window
import os

# 初始化 SparkSession
spark = SparkSession.builder.appName("CloudCourse-DoubanAnalysis").getOrCreate()

# 数据集路径（镜像内挂载路径）
DATA_PATH = "file:///opt/spark/work/data/movies.csv"

print(f"读取数据: {DATA_PATH}")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(DATA_PATH)

# ── A-1 数据清洗 ─────────────────────────────────────
print("原始 Schema:")
df.printSchema()
print("前 5 行:")
df.show(5, truncate=False)

total_rows = df.count()
print(f"原始行数: {total_rows}")

# 统计缺失值比例
print("各字段缺失值比例:")
for c in df.columns:
    missing = df.filter(col(c).isNull() | (col(c) == "")).count()
    print(f"  {c}: {missing} ({missing/total_rows:.2%})")

# 清洗策略：
# 1) year 为空 -> 用年份中位数填充
# 2) genres 为空 -> dropna
year_median = int(df.approxQuantile("year", [0.5], 0.01)[0])
print(f"年份中位数: {year_median}")

df_clean = df.fillna({"year": year_median}).dropna(subset=["genres"])

# 年份转成整数
df_clean = df_clean.withColumn("year", col("year").cast("int"))

print(f"清洗前行数: {total_rows}, 清洗后行数: {df_clean.count()}")

# 基本统计
numeric_cols = ["rating_score", "year"]
print("基本统计信息:")
df_clean.select(
    *[avg(c).alias(f"{c}_mean") for c in numeric_cols],
    *[stddev(c).alias(f"{c}_std") for c in numeric_cols],
    *[F_min(c).alias(f"{c}_min") for c in numeric_cols],
    *[F_max(c).alias(f"{c}_max") for c in numeric_cols]
).show()

# ── A-2 Spark SQL 统计分析 ───────────────────────────
# 注册临时视图
df_clean.createOrReplaceTempView("movies")

# 1. GROUP BY：按类型统计电影数量和平均评分
print("\n[查询1] 各类型电影数量与平均评分:")
q1 = spark.sql("""
    SELECT
        genre,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM (
        SELECT rating_score, explode(split(genres, '/')) AS genre FROM movies
    )
    GROUP BY genre
    ORDER BY movie_count DESC
""")
q1.show()

# 2. ORDER BY Top-N：评分最高的 10 部电影
print("\n[查询2] Top-10 评分电影:")
q2 = spark.sql("""
    SELECT title, rating_score, year
    FROM movies
    ORDER BY rating_score DESC, year DESC
    LIMIT 10
""")
q2.show(truncate=False)

# 3. 时间维度趋势：按年份统计电影数量和平均评分
print("\n[查询3] 按年份统计电影数量与平均评分:")
q3 = spark.sql("""
    SELECT
        year,
        COUNT(*) AS movie_count,
        ROUND(AVG(rating_score), 2) AS avg_rating
    FROM movies
    GROUP BY year
    ORDER BY year
""")
q3.show()

# 4. JOIN / 窗口函数：各类型评分 Top-3 电影
print("\n[查询4] 各类型评分 Top-3 电影（窗口函数）:")
genre_df = df_clean.withColumn("genre", explode(split(col("genres"), "/")))
window_spec = Window.partitionBy("genre").orderBy(col("rating_score").desc(), col("year").desc())
q4 = genre_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 3)
q4.select("genre", "title", "rating_score", "year", "rank").orderBy("genre", "rank").show(truncate=False)

spark.stop()
print("分析完成")

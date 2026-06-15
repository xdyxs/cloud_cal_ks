import time
import pandas as pd

CSV_PATH = "spark/data/movies.csv"

start = time.perf_counter()

df = pd.read_csv(CSV_PATH, encoding="utf-8")

clean = (
    df.drop_duplicates(subset=["movie_id"])
      .dropna(subset=["year", "rating_score"])
      .copy()
)

clean["genres"] = clean["genres"].fillna("未知类型")
clean["countries"] = clean["countries"].fillna("未知国家")

# Q6: Country-Genre Lift (Pandas implementation)
rows = []
for movie_id, countries, genres in clean[["movie_id", "countries", "genres"]].itertuples(index=False):
    for country in str(countries).split("/"):
        country = country.strip()
        if not country:
            continue
        for genre in str(genres).split("/"):
            genre = genre.strip()
            if genre and genre not in {"????",  "未知类型"}:
                rows.append((movie_id, country, genre))

pairs = pd.DataFrame(rows, columns=["movie_id", "country", "genre"])
pairs = pairs.drop_duplicates(["movie_id", "country", "genre"])

country_genre = (
    pairs.groupby(["country", "genre"], as_index=False)
         .agg(cnt=("movie_id", "nunique"))
)

country_total = (
    country_genre.groupby("country", as_index=False)
                 .agg(country_cnt=("cnt", "sum"))
)

global_genre = (
    country_genre.groupby("genre", as_index=False)
                 .agg(genre_cnt=("cnt", "sum"))
)

total_cnt = country_genre["cnt"].sum()

result = (
    country_genre.merge(country_total, on="country")
                 .merge(global_genre, on="genre")
)

result = result[
    (result["country_cnt"] >= 500)
    & (result["genre_cnt"] >= 300)
    & (result["cnt"] >= 50)
].copy()

result["country_share"] = result["cnt"] / result["country_cnt"]
result["global_share"] = result["genre_cnt"] / total_cnt
result["lift"] = result["country_share"] / result["global_share"]

result = result.sort_values("lift", ascending=False).head(20)

elapsed = time.perf_counter() - start

print(result.to_string(index=False))
print(f"\n耗时：{elapsed:.4f}s")

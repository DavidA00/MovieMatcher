import pandas as pd

base = pd.read_pickle("data/movie_base.pkl")

print("Total kept movies:", len(base))
print("Have MovieLens genres:", base["has_movielens_genres"].mean())
print("Have any ratings:", base["has_ratings"].mean())
print("Have reliable rating >=10:", base["has_reliable_rating"].mean())
print("Have any tags:", base["has_tags"].mean())

print("\nReliable average rating summary:")
print(base["avg_rating"].describe())

print("\nTop 20 most common movie-side tags:")
tag_rows = []
for _, row in base[["movieId", "top_tags"]].dropna().iterrows():
    for item in row["top_tags"]:
        tag_rows.append(item["tag"])

tag_df = pd.Series(tag_rows)
print(tag_df.value_counts().head(20))


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


years = base["year_movielens"].dropna().astype(int)

print("Min year:", years.min())
print("Max year:", years.max())

start = int(np.floor(years.min() / 5) * 5)
end = int(np.ceil(years.max() / 5) * 5) + 5
bins = np.arange(start, end, 5)

plt.figure(figsize=(12, 5))
plt.hist(years, bins=bins, edgecolor="black")
plt.xlabel("Release year")
plt.ylabel("Number of movies")
plt.title("Movie release years (5-year bins)")
plt.xticks(bins, rotation=45)
plt.tight_layout()
plt.show()

# optional tabular summary
year_bin = pd.cut(years, bins=bins, right=False)
summary = year_bin.value_counts().sort_index()

print("\nCounts by 5-year bin:")
for interval, count in summary.items():
    print(f"{interval.left:4d}-{interval.right - 1:4d}: {count}")
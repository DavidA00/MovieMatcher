# this is to explore the movielens dataset 32M version
from collections import Counter

import pandas as pd

movies = pd.read_csv("data/movielens/movies.csv")
ratings = pd.read_csv("data/movielens/ratings.csv")
links = pd.read_csv("data/movielens/links.csv")
tags = pd.read_csv("data/movielens/tags.csv")


print("Movies:", len(movies))
print("Ratings:", len(ratings))
print("Links:", len(links))
print("Tags:", len(tags))


print("\nSample movies")
print(movies.head())

print("\nSample ratings")
print(ratings.head())

print("\nSample links")
print(links.head())

print("\nSample tags")
print(tags.head())

print(f"Percentage of movies with no TMDB ID: {links['tmdbId'].isna().mean() * 100:.2f}%")

ratings_per_movie = ratings.groupby("movieId").size()
ratings_per_user = ratings.groupby("userId").size()

print("\nRatings per movie:")
print("  Mean:", ratings_per_movie.mean())
print("  Median:", ratings_per_movie.median())
print("  90th percentile:", ratings_per_movie.quantile(0.90))
print("  99th percentile:", ratings_per_movie.quantile(0.99))
print("  Movies with <= 2 ratings:", (ratings_per_movie <= 2).mean())
print("  Movies with <= 1 rating:", (ratings_per_movie <= 1).mean())

print("\nRatings per user:")
print("  Mean:", ratings_per_user.mean())
print("  Median:", ratings_per_user.median())
print("  90th percentile:", ratings_per_user.quantile(0.90))
print("  99th percentile:", ratings_per_user.quantile(0.99))

import matplotlib.pyplot as plt

movie_cutoff = ratings_per_movie.quantile(0.99)
user_cutoff = ratings_per_user.quantile(0.99)

plt.hist(ratings_per_movie[ratings_per_movie <= movie_cutoff], bins=100)
plt.xlabel("Number of ratings (clipped at 99th percentile)")
plt.ylabel("Number of movies")
plt.title("Distribution of ratings per movie (without extreme outliers)")
plt.show()

plt.hist(ratings_per_user[ratings_per_user <= user_cutoff], bins=100)
plt.xlabel("Number of ratings (clipped at 99th percentile)")
plt.ylabel("Number of users")
plt.title("Distribution of ratings per user (without extreme outliers)")
plt.show()

genre_counter = Counter()
for g in movies["genres"]:
    for genre in g.split("|"):
        genre_counter[genre] += 1

top_genres = genre_counter.most_common(15)
labels = [g for g, _ in top_genres]
values = [c for _, c in top_genres]

plt.bar(labels, values)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Genre")
plt.ylabel("Number of movies")
plt.title("Top 15 genres by movie count")
plt.tight_layout()
plt.show()

print("\nTop 15 genres by number of movies:")
for genre, count in top_genres:
    share = count / len(movies)
    print(f"  {genre:15s} {count:6d} ({share:5.1%})")

########################################################

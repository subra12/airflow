import pandas as pd

filepath_movie = r"C:\Users\achan\PycharmProjects\airflow\files\movies.csv"
filepath_rating = r"C:\Users\achan\PycharmProjects\airflow\files\ratings.csv"
filepath_tags = r"C:\Users\achan\PycharmProjects\airflow\files\tags.csv"


def get_data():
    df_movie = pd.read_csv(filepath_movie, nrows=10000)
    df_ratings = pd.read_csv(filepath_rating, nrows=10000)
    df_tags = pd.read_csv(filepath_tags, nrows=10000)
    return df_movie.head(5000), df_ratings.head(5000), df_tags.head(5000)


def merge_data(df_movie, df_ratings, df_tags):
    df_movie_ratings = df_movie.merge(df_ratings, how="inner", on="movieId")
    print(df_movie_ratings.columns)
    df_movie_tags = df_movie.merge(df_tags, how="inner", on="movieId")
    print(df_movie_tags.columns)
    df_movie_tags_ratings = df_movie_ratings.merge(df_movie_tags, how="inner", on="userId")
    print(df_movie_tags_ratings)
    return df_movie_ratings, df_tags, df_movie_tags_ratings






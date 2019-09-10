#!/usr/bin/env python3
from uuid import uuid4
from csv import DictReader

import hoply
from hoply.okvs.wiredtiger import WiredTiger


def make_uid():
    return uuid4().hex


triplestore = ("subject", "predicate", "object")
triplestore = hoply.open("movielens", prefix=[0], items=triplestore)


with WiredTiger("data/wt") as storage:
    with open("data/ml-20m/movies.csv") as f:
        movies = DictReader(f)
        with hoply.transaction(storage) as tr:
            for movie in movies:
                movieId = int(movie["movieId"])
                genres = movie["genres"].split("|")
                title = movie["title"]
                # add it
                uid = make_uid()
                triplestore.add(tr, uid, "movie/movieId", movieId)
                triplestore.add(tr, uid, "movie/title", title)
                for genre in genres:
                    triplestore.add(tr, uid, "movie/genre", genre)

    with open("data/ml-20m/ratings.csv") as f:
        ratings = DictReader(f)
        for index, rating in enumerate(ratings):
            if index % 10000 == 0:
                print(index)
                if index == 500_000:
                    break
            with hoply.transaction(storage) as tr:
                userId = int(rating["userId"])
                movieId = int(rating["movieId"])
                timestamp = int(rating["timestamp"])
                rating = float(rating["rating"])
                # add it
                uid = make_uid()
                triplestore.add(tr, uid, "rating/userId", userId)
                triplestore.add(tr, uid, "rating/movieId", movieId)
                triplestore.add(tr, uid, "rating/rating", rating)
                triplestore.add(tr, uid, "rating/timestamp", timestamp)

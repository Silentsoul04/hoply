#!/usr/bin/env python3
from csv import DictReader

import hoply
import ujson


with hoply.open('data/wt', dumps=ujson.dumps, loads=ujson.loads) as db:
    with open('data/ml-20m/movies.csv') as f:
        movies = DictReader(f)
        for movie in movies:
            movieId = int(movie['movieId'])
            genres = movie['genres'].split('|')
            title = movie['title']
            # add it
            uid = hoply.uid()
            db.add(uid, 'movie/movieId', movieId)
            db.add(uid, 'movie/title', title)
            for genre in genres:
                db.add(uid, 'movie/genre', genre)

    with open('data/ml-20m/ratings.csv') as f:
        ratings = DictReader(f)
        for index, rating in enumerate(ratings):
            if index % 10_000 == 0:
                print(index)
            userId = int(rating['userId'])
            movieId = int(rating['movieId'])
            timestamp = int(rating['timestamp'])
            rating = float(rating['rating'])
            # add it
            uid = hoply.uid()
            db.add(uid, 'rating/userId', userId)
            db.add(uid, 'rating/movieId', movieId)
            db.add(uid, 'rating/rating', rating)
            db.add(uid, 'rating/timestamp', timestamp)

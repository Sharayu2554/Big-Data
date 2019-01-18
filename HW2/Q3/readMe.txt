Q3

Movies.csv

movieId, title, genre

ratings.csv

userId, movieId, rating, timestamp

tags.csv

userId, movieId, tags, timestamp


Calculate the average ratings of each movie.
flatMap : (movieId, (rating, 1))
accumulator or reduceByKey (movieId, (sum of ratings, sum of counts))
average(movieId, sumOfRating, sumOfCount, Average)

Give the names of bottom 10 movies with lowest average ratings.

Input : average(movieId, sumOfRating, sumOfCount, Average)
takenOrder in desc on value avergae 

Find average rating of each movie where the movie’s tag is ‘action’.
create sql views of tags, rating, average, movies
select movieId, avergae from avergae , tags where tags = 'action' and tags.movieId = average.movieId

Find average rating of each movie where the movie’s tag is ‘action’  and  genre contains ‘thrill’.
create sql views of tags, rating, average, movies
select movieId, average from average , tags, movies where tags = 'action' and genre = 'thrill' and tags.movieId = average.movieId and tags.movieId = movies.movieId



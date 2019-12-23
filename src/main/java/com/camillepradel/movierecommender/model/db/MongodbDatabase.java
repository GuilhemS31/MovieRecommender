package com.camillepradel.movierecommender.model.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongodbDatabase extends AbstractDatabase {

	/**
	 * Init MongoDB
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection users --file movielens_movies.json 
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection movies --file movielens_users.json

"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection friends --file friends.csv --type=csv -fields=user1_id,user2_id
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection genres --file genres.csv --type=csv -fields=name,id
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection mov_genre --file mov_genre.csv --type=csv -fields=mov_id,genre
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection movies --file movies.csv --type=csv -fields=id,title,date
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection ratings --file ratings.csv --type=csv -fields=user_id,mov_id,rating,timestamp 
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection users --file users.csv --type=csv -fields=id,age,sex,occupation,zip_code

	 * Launch 
"C:\Program Files\MongoDB\Server\4.2\bin\mongo.exe" MoviesProj

	 * Delete for reset 
db.dropDatabase()
	 */

	//https://github.com/EtiennePer/MovieRecommender/blob/master/src/main/java/com/camillepradel/movierecommender/model/db/MongodbDatabase.java


	MongoClient mongoClient;

	public MongodbDatabase() {
		try {
			mongoClient = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	@Override
	public List<Movie> getAllMovies() {

		List<Movie> movies = new LinkedList<Movie>();

		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");
		DBCollection collectionMovies = db.getCollection("movies");

		//Iterate on all movies
		/**
		 * For create a list of movies, we should make the list of his genres
		 * For this, we should navigates on mov_genre collection with 2 consecutive requests
		 */
		Iterator<DBObject> cursorMovies = collectionMovies.find().iterator();
		while (cursorMovies.hasNext()) {
			final DBObject currentMovie = cursorMovies.next();
			movies.add(createMovie(db,currentMovie));
		}
		return movies;
	}

	@Override
	public List<Movie> getMoviesRatedByUser(int userId) {
		List<Movie> movies = new LinkedList<Movie>();

		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");
		DBCollection collectionRatings = db.getCollection("ratings");
		DBCollection collectionMovies = db.getCollection("movies");

		//Create a filter : ratings.ields=user_id = userId
		BasicDBObject userRatingsFilter = new BasicDBObject();
		userRatingsFilter.put("ields=user_id", userId);
		Iterator<DBObject> cursorRatings = collectionRatings.find(userRatingsFilter).iterator();
		while (cursorRatings.hasNext()) {
			final DBObject currentRating = cursorRatings.next();
			//Create a filter : ratings.mov_id = movies.ields=id
			BasicDBObject ratingMoviesFilter = new BasicDBObject();
			ratingMoviesFilter.put("ields=id", Integer.parseInt(currentRating.get("mov_id").toString()));
			DBObject currentMovie = collectionMovies.findOne(ratingMoviesFilter);
			movies.add(createMovie(db,currentMovie));

		}

		return movies;
	}

	@Override
	public List<Rating> getRatingsFromUser(int userId) {
		// TODO
		List<Rating> ratings = new LinkedList<Rating>();

		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");

		DBCollection collectionRatings = db.getCollection("ratings");
		DBCollection collectionMovies = db.getCollection("movies");

		//Create a filter : ratings.ields=user_id = userId
		BasicDBObject userRatingsFilter = new BasicDBObject();
		userRatingsFilter.put("ields=user_id", userId);
		Iterator<DBObject> cursorRatings = collectionRatings.find(userRatingsFilter).iterator();
		while (cursorRatings.hasNext()) {
			final DBObject currentRating = cursorRatings.next();
			//Create a filter : ratings.mov_id = movies.ields=id
			BasicDBObject ratingMoviesFilter = new BasicDBObject();
			ratingMoviesFilter.put("ields=id", Integer.parseInt(currentRating.get("mov_id").toString()));
			DBObject currentMovie = collectionMovies.findOne(ratingMoviesFilter);
			ratings.add(new Rating(createMovie(db,currentMovie), userId, Integer.parseInt(currentRating.get("rating").toString())));

		}
		return ratings;
	}

	@Override
	public void addOrUpdateRating(Rating rating) {
		// TODO: add query which
		// - add rating between specified user and movie if it doesn't exist
		// - update it if it does exist

		//look for create a Document with 2 filters -> rating.userID and rating.movie
		//si vide = créer un rating
		//si existe = voir pour update
	}

	@Override
	public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
		// TODO: process recommendations for specified user exploiting other users
		// ratings
		// use different methods depending on processingMode parameter
		Genre genre0 = new Genre(0, "genre0");
		Genre genre1 = new Genre(1, "genre1");
		Genre genre2 = new Genre(2, "genre2");
		List<Rating> recommendations = new LinkedList<Rating>();
		String titlePrefix;
		if (processingMode == 0) {
			titlePrefix = "0_";
		} else if (processingMode == 1) {
			titlePrefix = "1_";
		} else if (processingMode == 2) {
			titlePrefix = "2_";
		} else {
			titlePrefix = "default_";
		}
		recommendations.add(new Rating(
				new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[] { genre0, genre1 })), userId, 5));
		recommendations.add(new Rating(
				new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[] { genre0, genre2 })), userId, 5));
		recommendations.add(
				new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[] { genre1 })), userId, 4));
		recommendations.add(
				new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[] { genre0, genre1, genre2 })),
						userId, 3));
		return recommendations;
	}

	private Movie createMovie(DB db,DBObject currentMovie) {

		try {

			DBCollection collectionMovieGenres = db.getCollection("mov_genre");
			DBCollection collectionGenres = db.getCollection("genres");

			List<Genre> currentMovieGenres = new ArrayList<Genre>();

			//Create a filter : mov_genre.mov_id = movies.ields=id
			BasicDBObject movieGenreFilter = new BasicDBObject();
			movieGenreFilter.put("ields=mov_id", Integer.parseInt(currentMovie.get("ields=id").toString()));

			//Iterate on result = on genres associated at this movie
			Iterator<DBObject> cursorMovieGenre = collectionMovieGenres.find(movieGenreFilter).iterator();
			while (cursorMovieGenre.hasNext()) {
				final DBObject currentMovieGenreAssociation = cursorMovieGenre.next();

				//Create a filter : mov_genre.genre = genre.id
				BasicDBObject genreFilter = new BasicDBObject();
				genreFilter.put("id", Integer.parseInt(currentMovieGenreAssociation.get("genre").toString()));

				DBObject currentGenre = collectionGenres.findOne(genreFilter);
				//Get a genre and add it to the GenreList
				currentMovieGenres.add(
						new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("ields=name").toString()));
			}

			//Finaly create a Movie object with his attributes and his filled list of genre
			return new Movie(Integer.parseInt(currentMovie.get("ields=id").toString()),
					currentMovie.get("title").toString(), currentMovieGenres);
		}catch(NumberFormatException e) {
			//case of 1rst line of csv with colum name 
			System.out.println("Error while parsing int value, please look if current data is correct (some a wrong) : \n");
			e.printStackTrace();
		}catch(NullPointerException ee) {
			//case identified of something is null, don't know what
			System.out.println("Error while data, please have a look if current value is correct (some are null) : \n");
			ee.printStackTrace();
		}
		return null;
	}
}
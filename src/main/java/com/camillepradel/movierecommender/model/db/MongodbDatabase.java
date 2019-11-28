package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import scala.Array;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	
	
	// db connection info
	String url = "jdbc:neo4j:bolt://localhost:7687";
	String login = "admin";
	String password = "admin";

	// MongoDB Client
	MongoClient mongoClient;

	public MongodbDatabase() {
		try {
			mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Movie> getAllMovies() {
		// TODO: test
		List<Movie> movies = new LinkedList<Movie>();

		DB database = mongoClient.getDB("MoviesProj");
		DBCollection collectionMovies = database.getCollection("movies");

		DBCursor cursor = collectionMovies.find();
		while (cursor.hasNext()) {
			final DBObject currentMovie = cursor.next();

			List<Genre> currentMovieGenres = new LinkedList<Genre>();

			DBCollection collectionMovieGenres = database.getCollection("mov_genre");

			// filter collectionMovieGenres mov_id with currentMovie.get("id")
			BasicDBObject filterMovID = new BasicDBObject();
			filterMovID.put("mov_id",currentMovie.get("id").toString());
			DBCursor cursor2 = collectionMovieGenres.find(new BasicDBObject(), filterMovID);
			
			while (cursor2.hasNext()) {
				final DBObject currentMovieGenre = cursor2.next();
				DBCollection collectionGenres = database.getCollection("genres");
				// filter collectionMovieGenres mov_id with currentMovie.get("id")
				BasicDBObject filterGenreID = new BasicDBObject();
				filterGenreID.put("id",currentMovieGenre.get("id").toString());
				DBCursor cursor3 = collectionGenres.find(new BasicDBObject(), filterGenreID);
				while (cursor3.hasNext()) {
					final DBObject currentGenre = cursor3.next();
					currentMovieGenres.add(new Genre(Integer.parseInt(currentGenre.get("id").toString()),
							currentGenre.get("name").toString()));
				}
			}

			movies.add(new Movie(Integer.parseInt(currentMovie.get("id").toString()),
					currentMovie.get("title").toString(), currentMovieGenres));
		}
		return movies;
	}

	@Override
	public List<Movie> getMoviesRatedByUser(int userId) {
		// TODO: write query to retrieve all movies rated by user with id userId
		List<Movie> movies = new LinkedList<Movie>();
		Genre genre0 = new Genre(0, "genre0");
		Genre genre1 = new Genre(1, "genre1");
		Genre genre2 = new Genre(2, "genre2");
		movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[] { genre0, genre1 })));
		movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[] { genre0, genre1, genre2 })));
		return movies;
	}

	@Override
	public List<Rating> getRatingsFromUser(int userId) {
		// TODO: write query to retrieve all ratings from user with id userId
		List<Rating> ratings = new LinkedList<Rating>();
		Genre genre0 = new Genre(0, "genre0");
		Genre genre1 = new Genre(1, "genre1");
		ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[] { genre0, genre1 })), userId, 3));
		ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[] { genre1 })), userId, 4));
		return ratings;
	}

	@Override
	public void addOrUpdateRating(Rating rating) {
		// TODO: add query which
		// - add rating between specified user and movie if it doesn't exist
		// - update it if it does exist
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

//	private List<Movie> getMoviesFromDB() {
//
//		List<Movie> movies = new LinkedList<Movie>();
//		DB database = mongoClient.getDB("MoviesProj");
//		DBCollection collectionMovies = database.getCollection("movies");
//		
//		List<Genre> genres = getGenresFromDB();
//		
//		DBCursor cursor = collectionMovies.find();
//		while (cursor.hasNext()) {
//			final DBObject currentMovie = cursor.next();
//			movies.add(new Movie(Integer.parseInt(currentMovie.get("id").toString()),
//					currentMovie.get("title").toString(),
//					new ArrayList<Genre>()));
//		}
//
//		return movies;
//	}
//
//	private List<Genre> getGenresFromDB() {
//
//		List<Genre> genres = new LinkedList<Genre>();
//		DB database = mongoClient.getDB("MoviesProj");
//		DBCollection collectionMovies = database.getCollection("genres");
//
//		DBCursor cursor = collectionMovies.find();
//		while (cursor.hasNext()) {
//			final DBObject currentMovie = cursor.next();
//
//			currentMovie.get("id");
//		}
//
//		return genres;
//	}
//
//	private List<Rating> getRatingsFromDB() {
//
//		List<Rating> ratings = new LinkedList<Rating>();
//		DB database = mongoClient.getDB("MoviesProj");
//		DBCollection collectionMovies = database.getCollection("ratings");
//
//		DBCursor cursor = collectionMovies.find();
//		while (cursor.hasNext()) {
//			final DBObject currentMovie = cursor.next();
//
//			currentMovie.get("id");
//		}
//
//		return ratings;
//	}
}
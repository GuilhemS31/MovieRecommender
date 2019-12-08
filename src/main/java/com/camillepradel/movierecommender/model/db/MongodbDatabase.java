package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.DBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import scala.Array;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

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
	String login = "root";
	String password = "root";
	String host = "localhost";
	String adminDB = "admin";

	// MongoDB Client
	ConnectionString connString = new ConnectionString(
			"mongodb://localhost:27017");
			//"mongodb://" + password + ":" + login + "@" + host);  
	//"?w=majority"
	//?authSource=admin
	MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connString).retryWrites(true)
			.build();
	MongoClient mongoClient = MongoClients.create(settings);
	
	@Override
	public List<Movie> getAllMovies() {
		
		List<Movie> movies = new LinkedList<Movie>();

		MongoDatabase database = mongoClient.getDatabase("MovieProj");
		MongoCollection<Document> collectionMovies = database.getCollection("movies");
		MongoCollection<Document> collectionMovieGenres = database.getCollection("mov_genre");
		MongoCollection<Document> collectionGenres = database.getCollection("genres");

		//parsing all movies
		for (Document currentMovie : collectionMovies.find()) {// TODO: .find() give nothing
			List<Genre> currentMovieGenres = new ArrayList<Genre>();

			//collect mov_genre association corresponding to this movie
			for (Document associatedGenresToThisMovie : collectionMovieGenres
					.find(new Document("mov_id", currentMovie.get("id").toString()))) {
				//find genres and add them in a list for create the Movie object
				for (Document currentGenre : collectionGenres
						.find(new Document("id", associatedGenresToThisMovie.get("genre")))) {
					currentMovieGenres.add(
							new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("name").toString()));
				}
			}
			//Add to result 
			movies.add(new Movie(Integer.parseInt(currentMovie.get("id").toString()),
					currentMovie.get("title").toString(), currentMovieGenres));
		}
		return movies;
	}

	@Override
	public List<Movie> getMoviesRatedByUser(int userId) {
		// TODO: to test
		List<Movie> movies = new LinkedList<Movie>();

		MongoDatabase database = mongoClient.getDatabase("MovieProj");
		MongoCollection<Document> collectionRatings = database.getCollection("ratings");
		MongoCollection<Document> collectionMovies = database.getCollection("movies");
		MongoCollection<Document> collectionMovieGenres = database.getCollection("mov_genre");
		MongoCollection<Document> collectionGenres = database.getCollection("genres");

		//find ratings for given user
		for (Document currentRating : collectionRatings.find(new Document("user_id", userId))) {
			//get movies from ratings selected
			//then same code than getAllMovies() but with filtered movies
			for (Document currentMovie : collectionMovies.find(
					new Document("id", currentRating.get("mov_id")))) {
				List<Genre> currentMovieGenres = new ArrayList<Genre>();
				
				for (Document associatedGenresToThisMovie : collectionMovieGenres
						.find(new Document("mov_id", currentMovie.get("id").toString()))) {
					for (Document currentGenre : collectionGenres
							.find(new Document("id", associatedGenresToThisMovie.get("genre")))) {
						currentMovieGenres.add(
								new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("name").toString()));
					}
				}

				movies.add(new Movie(Integer.parseInt(currentMovie.get("id").toString()),
						currentMovie.get("title").toString(), currentMovieGenres));
			}
		}
		return movies;
	}

	@Override
	public List<Rating> getRatingsFromUser(int userId) {
		// TODO: to test
		List<Rating> ratings = new LinkedList<Rating>();

		MongoDatabase database = mongoClient.getDatabase("MovieProj");
		MongoCollection<Document> collectionRatings = database.getCollection("ratings");
		MongoCollection<Document> collectionMovies = database.getCollection("movies");
		MongoCollection<Document> collectionMovieGenres = database.getCollection("mov_genre");
		MongoCollection<Document> collectionGenres = database.getCollection("genres");

		//find ratings for given user
		for (Document currentRating : collectionRatings.find(new Document("user_id", userId))) {
			//get movies from ratings selected
			//then same code than getAllMovies() but with filtered movies
			for (Document currentMovie : collectionMovies.find(
					new Document("id", currentRating.get("mov_id")))) {
				List<Genre> currentMovieGenres = new ArrayList<Genre>();
				
				for (Document associatedGenresToThisMovie : collectionMovieGenres
						.find(new Document("mov_id", currentMovie.get("id").toString()))) {
					for (Document currentGenre : collectionGenres
							.find(new Document("id", associatedGenresToThisMovie.get("genre")))) {
						currentMovieGenres.add(
								new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("name").toString()));
					}
				}
				//create a rating object by creating a Movie
				ratings.add(new Rating(new Movie(Integer.parseInt(currentMovie.get("id").toString()),
						currentMovie.get("title").toString(), currentMovieGenres),
						userId, 
						Integer.parseInt(currentRating.get("rating").toString())));
			}
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
}
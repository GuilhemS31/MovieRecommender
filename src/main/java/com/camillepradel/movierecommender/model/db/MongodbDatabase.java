package com.camillepradel.movierecommender.model.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
json
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection users --file movielens_movies.json 
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection movies --file movielens_users.json
csv
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection friends --file friends.csv --type=csv --fields=user1_id,user2_id
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection genres --file genres.csv --type=csv --fields=name,id
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection mov_genre --file mov_genre.csv --type=csv --fields=mov_id,genre
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection movies --file movies.csv --type=csv --fields=id,title,date
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection ratings --file ratings.csv --type=csv --fields=user_id,mov_id,rating,timestamp 
"C:\Program Files\MongoDB\Server\4.2\bin\mongoimport.exe" --db MoviesProj --collection users --file users.csv --type=csv --fields=id,age,sex,occupation,zip_code

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

		//Create a filter : ratings.user_id = userId
		BasicDBObject userRatingsFilter = new BasicDBObject();
		userRatingsFilter.put("user_id", userId);
		Iterator<DBObject> cursorRatings = collectionRatings.find(userRatingsFilter).iterator();
		while (cursorRatings.hasNext()) {
			final DBObject currentRating = cursorRatings.next();
			//Create a filter : ratings.mov_id = movies.id
			BasicDBObject ratingMoviesFilter = new BasicDBObject();
			ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
			DBObject currentMovie = collectionMovies.findOne(ratingMoviesFilter);
			movies.add(createMovie(db,currentMovie));

		}

		return movies;
	}

	@Override
	public List<Rating> getRatingsFromUser(int userId) {
		List<Rating> ratings = new LinkedList<Rating>();

		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");

		DBCollection collectionRatings = db.getCollection("ratings");
		DBCollection collectionMovies = db.getCollection("movies");

		//Create a filter : ratings.user_id = userId
		BasicDBObject userRatingsFilter = new BasicDBObject();
		userRatingsFilter.put("user_id", userId);
		Iterator<DBObject> cursorRatings = collectionRatings.find(userRatingsFilter).iterator();
		while (cursorRatings.hasNext()) {
			final DBObject currentRating = cursorRatings.next();
			//Create a filter : ratings.mov_id = movies.id
			BasicDBObject ratingMoviesFilter = new BasicDBObject();
			ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
			DBObject currentMovie = collectionMovies.findOne(ratingMoviesFilter);
			ratings.add(new Rating(createMovie(db,currentMovie), userId, Integer.parseInt(currentRating.get("rating").toString())));

		}
		return ratings;
	}

	@Override
	public void addOrUpdateRating(Rating rating) {
		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");

		DBCollection collectionRatings = db.getCollection("ratings");

		//Create a filter : ratings.user_id = rating.user_id and ratings.mov_id = rating.mov_id
		//For get the User-Movie ratings, if no relation = create else update
		BasicDBObject ratingFilter = new BasicDBObject();
		ratingFilter.put("user_id", rating.getUserId());
		ratingFilter.put("mov_id", rating.getMovieId());
		DBObject cursorRatings = collectionRatings.findOne(ratingFilter);
		if(cursorRatings != null) {
			//Relation User - Movie already exist, update the rating

			BasicDBObject updateFields = new BasicDBObject();
			updateFields.append("rating", rating.getScore());
			//			updateFields.append("timestamp", System.currentTimeMillis());

			BasicDBObject setQuery = new BasicDBObject();
			setQuery.append("$set", updateFields);

			collectionRatings.update(ratingFilter, setQuery);
		}
		else {
			//Relation User - Movie don't exist, insert the rating

			BasicDBObject createFields = new BasicDBObject();
			createFields.append("user_id", rating.getUserId());
			createFields.append("mov_id", rating.getMovieId());
			createFields.append("rating", rating.getScore());
			//			createFields.append("timestamp", System.currentTimeMillis());

			collectionRatings.insert(createFields);
		}
	}

	@Override
	public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
		

		List<Rating> recommendations = new LinkedList<Rating>();

		//TODO 0 and 1 : get user movies here and remove common ratings
		if (processingMode == 0) {
			//Variante 1 : l’utilisateur le plus proche
			recommendations = getRatingsFromUser(findUserProches(userId,1).get(0));
		} else if (processingMode == 1) {
			//Variante 2: les cinq utilisateurs les plus proches
			for(int i : findUserProches(userId,5)){
				recommendations.addAll(getRatingsFromUser(i));
			}
		} else if (processingMode == 2) {
			// TODO
			//Variante 3: prise en compte de la valeur des scores
		}
		return recommendations;
	}

	private Movie createMovie(DB db,DBObject currentMovie) {

		try {

			DBCollection collectionMovieGenres = db.getCollection("mov_genre");
			DBCollection collectionGenres = db.getCollection("genres");

			List<Genre> currentMovieGenres = new ArrayList<Genre>();

			//Create a filter : mov_genre.mov_id = movies.id
			BasicDBObject movieGenreFilter = new BasicDBObject();
			movieGenreFilter.put("mov_id", Integer.parseInt(currentMovie.get("id").toString()));

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
						new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("name").toString()));
			}

			//Finaly create a Movie object with his attributes and his filled list of genre
			return new Movie(Integer.parseInt(currentMovie.get("id").toString()),
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

	private List<Integer> findUserProches(int userId,int nbr) {
		List<Integer> usersProchesIDs = new ArrayList<Integer>();

		//Get all movies rated by user
		List<Movie> moviesRatedByUser = getMoviesRatedByUser(userId);
		//and their IDs
		List<Integer> moviesIdRatedByUser = new ArrayList<Integer>(); 
		for(Movie currentMovie : moviesRatedByUser){
			moviesIdRatedByUser.add(currentMovie.getId());
		}

		//Init calculs storage
		Map<Integer,Integer> user_sameMovie_Map = new HashMap<Integer,Integer>();

		//Get DB collections
		DB db = mongoClient.getDB("MoviesProj");

		DBCollection collectionRatings = db.getCollection("ratings");

		//parse ratings and count tuple (user , common movies)
		Iterator<DBObject> cursorRatings = collectionRatings.find().iterator();
		while (cursorRatings.hasNext()) {
			try {
				final DBObject currentRating = cursorRatings.next();
				if(moviesIdRatedByUser.contains(Integer.parseInt(currentRating.get("mov_id").toString()))) {
					//if currentRating is  about a movie rated by userId
					int currentUser = Integer.parseInt(currentRating.get("user_id").toString());
					if(currentUser != userId && user_sameMovie_Map.keySet().contains(currentUser)) {
						//if currentUser has already a common movie with userId
						int newValue = user_sameMovie_Map.get(currentUser) + 1;
						user_sameMovie_Map.put(currentUser, newValue);
					}
					else {
						user_sameMovie_Map.put(currentUser, 1);
					}
				}
			}catch(NumberFormatException e) {
				//case of 1rst line of csv with colum name 
				System.out.println("Error while parsing int value, please look if current data is correct (some a wrong) : \n");
				e.printStackTrace();
			}
		}

		int i = 0;
		do {
			i++;
			Map.Entry<Integer,Integer> maxEntry = null;

			for (Map.Entry<Integer,Integer> entry : user_sameMovie_Map.entrySet()){
				if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0){
					maxEntry = entry;
				}
			}

			usersProchesIDs.add(maxEntry.getKey());
			user_sameMovie_Map.remove(maxEntry.getKey());
		}while(i < nbr);

		System.out.println(usersProchesIDs);
		return usersProchesIDs;
	}
}
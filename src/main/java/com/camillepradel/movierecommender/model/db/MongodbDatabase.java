package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MongodbDatabase extends AbstractDatabase {
	
	/** 
	* Init MongoDB 

{PathTo : mongoimport.exe} --db MoviesProj --collection friends --file friends.csv --type=csv -fields=user1_id,user2_id
{PathTo : mongoimport.exe} --db MoviesProj --collection genres --file genres.csv --type=csv -fields=name,id
{PathTo : mongoimport.exe} --db MoviesProj --collection mov_genre --file mov_genre.csv --type=csv -fields=mov_id,genre
{PathTo : mongoimport.exe} --db MoviesProj --collection movies --file movies.csv --type=csv -fields=id,title,date
{PathTo : mongoimport.exe} --db MoviesProj --collection ratings --file ratings.csv --type=csv -fields=user_id,mov_id,rating,timestamp 
{PathTo : mongoimport.exe} --db MoviesProj --collection users --file users.csv --type=csv -fields=id,age,sex,occupation,zip_code 

	* Launch
{PathTo : mongo.exe} MoviesProj
	
	* Delete for reset
db.dropDatabase()
	 */
	
    // db connection info
    String url = "jdbc:neo4j:bolt://localhost:7687";
    String login = "admin";
    String password = "admin";
    
    //MongoDB Client
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
		return moviesFromDB();
	}

	@Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: test
        List<Movie> movies = new LinkedList<Movie>();
        List<Rating> ratings = ratingsFromDB();

        for(Rating currentRating : ratings) {
        	if(currentRating.getUserId() == userId) {
        		movies.add(currentRating.getMovie());
        	}
        }
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: test
        List<Rating> ratings = ratingsFromDB();
        List<Rating> allratings = ratingsFromDB();

        for(Rating currentRating : allratings) {
        	if(currentRating.getUserId() == userId) {
        		ratings.add(currentRating);
        	}
        }
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist

        List<Rating> allratings = ratingsFromDB();
        boolean finded = false;
        
        for(Rating currentRating : allratings) {
        	if(currentRating.getUserId() == rating.getUserId() 
        			&& currentRating.getMovieId() == rating.getMovieId()) {
        		//Rating finded, update it
        		//TODO mongo UPDATE
        		finded = true;
        	}
        }

		//Rating not finded, create it
        if(!finded) {
        	
        }
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
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
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }    
    
    


    private List<Movie> moviesFromDB() {
    	List<Movie> result = new ArrayList<Movie>();


		DB database = mongoClient.getDB("MoviesProj");
		DBCollection collectionMovies = database.getCollection("movies");
		DBCollection collectionMovGenre = database.getCollection("mov_genre");
		
    	List<Genre> allGenres = genresFromDB();
		
		final Iterator<DBObject> cursor = collectionMovies.find();
		while (cursor.hasNext()) {
			//current Movie from movies Collection
			final DBObject currentMovie = cursor.next();
			
			//get basic attribute
			final int movieID = Integer.parseInt(currentMovie.get("id").toString());
			final String movieTitle = currentMovie.get("title").toString();
			//final String movieDate = currentMovie.get("date").toString();
			final List<Genre> movieGenres = new ArrayList<Genre>();
			
			//get list of genre from mov_genre and genre Collection
			final Iterator<DBObject> cursor2 = collectionMovGenre.find();
			while (cursor2.hasNext()) {
				final DBObject currentMovGen = cursor2.next();
					if(movieID == Integer.parseInt(currentMovGen.get("mov_id").toString())) {
						for(Genre currentGenre : allGenres) {
							if(currentGenre.getId() == Integer.parseInt(currentMovGen.get("genre").toString())){
								movieGenres.add(currentGenre);
							}
						}
					}
			}
			result.add(new Movie(movieID, movieTitle, movieGenres));
		}
		
		return result;
	}

    private List<Genre> genresFromDB() {
    	List<Genre> result = new ArrayList<Genre>();

    	DB database = mongoClient.getDB("MoviesProj");
		DBCollection collectionGenres = database.getCollection("genres");
		
		final Iterator<DBObject> cursor = collectionGenres.find();
		while (cursor.hasNext()) {
			//current Genre from genre Collection
			final DBObject currentMovie = cursor.next();

			final int genreID = Integer.parseInt(currentMovie.get("id").toString());
			final String genreName = currentMovie.get("name").toString();
			
			result.add(new Genre(genreID,genreName));
		}
			
		return result;
    }
    
    private List<Rating> ratingsFromDB() {
    	List<Rating> result = new ArrayList<Rating>();

    	DB database = mongoClient.getDB("MoviesProj");
		DBCollection collectionGenres = database.getCollection("ratings");

    	List<Movie> allMovies = moviesFromDB();
    	
		final Iterator<DBObject> cursor = collectionGenres.find();
		while (cursor.hasNext()) {
			//current Genre from genre Collection
			final DBObject currentRating = cursor.next();

			final int userID = Integer.parseInt(currentRating.get("user_id").toString());
			final int movieID = Integer.parseInt(currentRating.get("mov_id").toString());
			final int score = Integer.parseInt(currentRating.get("rating").toString());
			//final int timestamp = Integer.parseInt(currentMovie.get("timestamp").toString());
			Movie movie = null;
			
			for(Movie currentMovie : allMovies) {
				if(currentMovie.getId() == userID) {
					movie = currentMovie;
				}
			}
			result.add(new Rating(movie,userID,score));
		}
		
		return result;
    }
    
}

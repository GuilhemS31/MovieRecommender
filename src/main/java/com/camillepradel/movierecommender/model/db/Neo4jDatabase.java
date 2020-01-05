package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class Neo4jDatabase extends AbstractDatabase {
	
	Driver driver = null;

    // db connection info
    String url = "bolt://localhost:7687";
    String login = "admin";
    String password = "admin";
	
    public Neo4jDatabase() {
        // load JDBC driver
        try {
            Class.forName("org.neo4j.jdbc.bolt.BoltDriver");            
        } catch (ClassNotFoundException e) {
			e.printStackTrace();
		}


        driver = GraphDatabase.driver(url, AuthTokens.basic(login, password));
    }
    
    @Override
    public List<Movie> getAllMovies() {
    	List<Movie> movies = new LinkedList<Movie>();
        
        Session session = driver.session();
        
        StatementResult result = session.run( "MATCH (m:Movie)-[r]->(g:Genre) WHERE type(r) = 'CATEGORIZED_AS' RETURN m.id,m.title, collect(g.name) as g_name ORDER BY m.id" );
        
        
        while ( result.hasNext() )
        {
            Record record = result.next();
            int id = record.get("m.id").asInt();
            String titre = record.get("m.title").asString();
            
            List<Object> listGenre = record.get("g_name").asList();
            List<Genre> listGenreMieu = new ArrayList<Genre>();
            for(int i=0; i < listGenre.size(); i++) {
                StatementResult resultBis = session.run("Match (g:Genre {name:\""+ listGenre.get(i).toString() + "\"}) return g.id");
                Record recordGenre = resultBis.single();
                int genreId = recordGenre.get("g.id").asInt();
            	listGenreMieu.add(new Genre(genreId, listGenre.get(i).toString()));
            }
            
            movies.add(new Movie(id, titre, listGenreMieu));
        }

        session.close();
        
         return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        List<Movie> movies = new LinkedList<Movie>();
        
        Session session = driver.session();
        
        StatementResult result = session.run("MATCH (u:User{id:" + userId + "})-[r:RATED]->(m:Movie)-[r2:CATEGORIZED_AS]->(g:Genre) RETURN m.id, m.title, collect(g.name) as g_name");
        
        while ( result.hasNext() )
        {
            Record record = result.next();
            int id = record.get("m.id").asInt();
            String titre = record.get("m.title").asString();
            
            List<Object> listGenre = record.get("g_name").asList();
            List<Genre> listGenreMieu = new ArrayList<Genre>();
            for(int i=0; i < listGenre.size(); i++) {
                StatementResult resultBis = session.run("Match (g:Genre {name:\""+ listGenre.get(i).toString() + "\"}) return g.id");
                Record recordGenre = resultBis.single();
                int genreId = recordGenre.get("g.id").asInt();
            	listGenreMieu.add(new Genre(genreId, listGenre.get(i).toString()));
            }
            
            movies.add(new Movie(id, titre, listGenreMieu));
        }

        session.close();
        
        return movies;
        
        
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        List<Rating> ratings = new LinkedList<Rating>();
        
        Session session = driver.session();
        
        StatementResult result = session.run("MATCH (u:User{id:"+ userId + "})-[r:RATED]->(m:Movie)-[r2:CATEGORIZED_AS]->(g:Genre) Return r.note , m.id, m.title, collect(g.name) as g_name");
        
        while ( result.hasNext() )
        {
            Record record = result.next();
            int score = record.get("r.note").asInt();
            int id = record.get("m.id").asInt();
            String titre = record.get("m.title").asString();
            
            List<Object> listGenre = record.get("g_name").asList();
            List<Genre> listGenreMieu = new ArrayList<Genre>();
            for(int i=0; i < listGenre.size(); i++) {
                StatementResult resultBis = session.run("Match (g:Genre {name:\""+ listGenre.get(i).toString() + "\"}) return g.id");
                Record recordGenre = resultBis.single();
                int genreId = recordGenre.get("g.id").asInt();
            	listGenreMieu.add(new Genre(genreId, listGenre.get(i).toString()));
            }
            ratings.add(new Rating(new Movie(id, titre, listGenreMieu), userId, score));
        }

        session.close();
        
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
    	int userId = rating.getUserId();
    	int movieId = rating.getMovieId();
    	int note = rating.getScore();
    	
    	Session session = driver.session();
    	
    	//check if movie has already been rated by user
        StatementResult result = session.run("MATCH (u:User{id:" + userId + "})-[r:RATED]->(m:Movie{id:" + movieId + "}) RETURN u.id, r.note, m.title");
        if ( result.hasNext() ) {
        	//update rating
        	session.run("MATCH (u:User{id:" + userId + "})-[r:RATED]->(m:Movie{id:" + movieId + "}) SET r.note = " + note + " RETURN u.id, r.note, m.title");
        }
        else {
        	//create rating
        	session.run("MATCH (u:User{id:" + userId + "}), (m:Movie{id:" + movieId + "}) CREATE (u)-[r:RATED {note:" + note + ", timestamp:" + System.currentTimeMillis() + "}]->(m) RETURN u.id, r.note, m.title");
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
            recommendations = this.processRecommendationsForUsersPM1(userId);
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        return recommendations;
    }
    
    private List<Rating> processRecommendationsForUsersPM1(int userId) {
    	List<Rating> recommendations = new LinkedList<Rating>();
    	
    	Session session = driver.session();
    	
    	String querry = "MATCH (target_user:User {id :" + userId + "})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) WITH other_user, count(distinct m.title) AS num_common_movies, target_user ORDER BY num_common_movies DESC LIMIT 1 MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)-[r2:CATEGORIZED_AS]->(g:Genre) WHERE NOT (target_user)-[:RATED]->(m2) RETURN m2.id, m2.title AS rec_movie_title, collect(g.name) AS g_name, rat_other_user.note AS rating, other_user.id AS watched_by ORDER BY rat_other_user.note DESC";
        StatementResult result = session.run(querry);
        
        while ( result.hasNext() )
        {
        	Record record = result.next();
        	int otherUserId = record.get("watched_by").asInt();
            int score = record.get("rating").asInt();
            int movieId = record.get("m2.id").asInt();
        	String movieTitle = record.get("rec_movie_title").asString();
        	
        	List<Object> listGenre = record.get("g_name").asList();
            List<Genre> listGenreMieu = new ArrayList<Genre>();
            for(int i=0; i < listGenre.size(); i++) {
                StatementResult resultBis = session.run("Match (g:Genre {name:\""+ listGenre.get(i).toString() + "\"}) return g.id");
                Record recordGenre = resultBis.single();
                int genreId = recordGenre.get("g.id").asInt();
            	listGenreMieu.add(new Genre(genreId, listGenre.get(i).toString()));
            }
        	
        	Movie movie = new Movie(movieId, movieTitle, listGenreMieu);
        	Rating recommendation = new Rating(movie, otherUserId, score);
        	recommendations.add(recommendation);
        }
        
        session.close();
    	
    	return recommendations;
    }
    
    private List<Rating> processRecommendationsForUsersPM2(int userId) {
    	List<Rating> recommendations = new LinkedList<Rating>();
    	
    	Session session = driver.session();
    
    	String querry = "MATCH (target_user:User {id :" + userId + "})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) WITH other_user, count(distinct m.title) AS num_common_movies, target_user ORDER BY num_common_movies DESC LIMIT 5 MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)-[r2:CATEGORIZED_AS]->(g:Genre) WHERE NOT (target_user)-[:RATED]->(m2) RETURN m2.id, m2.title AS rec_movie_title, collect(g.name) AS g_name, rat_other_user.note AS rating, other_user.id AS watched_by ORDER BY rat_other_user DESC";
    	StatementResult result = session.run(querry);
        
        while ( result.hasNext() )
        {
        	Record record = result.next();
        	int otherUserId = record.get("watched_by").asInt();
            int score = record.get("rating").asInt();
            int movieId = record.get("m2.id").asInt();
        	String movieTitle = record.get("rec_movie_title").asString();
        	
        	List<Object> listGenre = record.get("g_name").asList();
            List<Genre> listGenreMieu = new ArrayList<Genre>();
            for(int i=0; i < listGenre.size(); i++) {
                StatementResult resultBis = session.run("Match (g:Genre {name:\""+ listGenre.get(i).toString() + "\"}) return g.id");
                Record recordGenre = resultBis.single();
                int genreId = recordGenre.get("g.id").asInt();
            	listGenreMieu.add(new Genre(genreId, listGenre.get(i).toString()));
            }
        	
        	Movie movie = new Movie(movieId, movieTitle, listGenreMieu);
        	Rating recommendation = new Rating(movie, otherUserId, score);
        	recommendations.add(recommendation);
        }
        
        session.close();
    	
    	return recommendations;
    }
}

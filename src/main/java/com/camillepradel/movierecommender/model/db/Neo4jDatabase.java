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
    	System.out.println("getAllMovies");
    	
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
        // TODO: write query to retrieve all movies rated by user with id userId
        List<Movie> movies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        List<Rating> ratings = new LinkedList<Rating>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
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
    

}

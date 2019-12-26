package com.camillepradel.movierecommender.testscript;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BenchMarkMovieRecommander {
	
	static List<String> benchmarkResults = new ArrayList<String>();
	
    public static void main(String[] args) {

        long startTime = System.nanoTime();
        
        testMovieRecommanderService("http://localhost:8080/MovieRecommender/movies?user_id=");
        testMovieRecommanderService("http://localhost:8080/MovieRecommender/movieratings?user_id=");
        testMovieRecommanderService("http://localhost:8080/MovieRecommender/recommendations?processing_mode=0&user_id=");
        testMovieRecommanderService("http://localhost:8080/MovieRecommender/recommendations?processing_mode=1&user_id=");
        testMovieRecommanderService("http://localhost:8080/MovieRecommender/recommendations?processing_mode=2&user_id=");
    	
    	long endTime = System.nanoTime();
        double time = (double) (endTime - startTime) / 1000000000.;
        benchmarkResults.add("Global benchmark duration : " + time);
        
        try {
			PrintWriter out = new PrintWriter("benchmarkLogs.txt");
	        
	        for(String currentResult : benchmarkResults) {
	        	System.out.println(currentResult);
	        	out.println(currentResult);
	        }
	        
	        out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    
    private static void testMovieRecommanderService(String urlStart) {
    	benchmarkResults.add("-- BEGIN URL test " + urlStart);
    	
        List<Integer> userIdsToTest = new ArrayList<Integer>();
        userIdsToTest.add(1);// Massive data
        userIdsToTest.add(63);//Low data

        for(int nbIter = 1; nbIter < 1001; nbIter = nbIter * 10) {
            long startIterationTime = System.nanoTime();
            
            for(int i = 0; i < nbIter; i++) {
            	benchmarkResults.add("-- BEGIN iteration " + i);
                long startIterTime = System.nanoTime();
            	for(Integer currentUserId : userIdsToTest) {
                	benchmarkResults.add("-- BEGIN test on user " + currentUserId);
                    long startUserTime = System.nanoTime();
                    
                    URL u;
                    InputStream is = null;
                    DataInputStream dis;

                    try {
                        u = new URL(urlStart + currentUserId);
                        is = u.openStream();
                        dis = new DataInputStream(new BufferedInputStream(is));
                        while (dis.readLine() != null) {
                        }
                    } catch (MalformedURLException mue) {
                    	benchmarkResults.add("Ouch - a MalformedURLException happened.");
                    	benchmarkResults.add(mue.getMessage());
                    } catch (IOException ioe) {
                    	benchmarkResults.add("Oops- an IOException happened.");
                    	benchmarkResults.add(ioe.getMessage());
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ioe) {
                        }
                    }
                    
                    long endUserTime = System.nanoTime();
                    double timeUser = (double) (endUserTime - startUserTime) / 1000000000.;
                	benchmarkResults.add("Time to process current user : " + timeUser);
                	benchmarkResults.add("END test on user " + currentUserId);
                }

                long endIterTime = System.nanoTime();
                double timeIter = (double) (endIterTime - startIterTime) / 1000000000.;
            	benchmarkResults.add("Time to process current user : " + timeIter);
            	benchmarkResults.add("END iter " + i);
            }
            
            long endIterationTime = System.nanoTime();
            double timeIteration = (double) (endIterationTime - startIterationTime) / 1000000000.;
            benchmarkResults.add("Time to process " + nbIter + " requests in one thread: " + timeIteration + "s");
        }
    	benchmarkResults.add("END URL test " + urlStart +"\n");
    }
}

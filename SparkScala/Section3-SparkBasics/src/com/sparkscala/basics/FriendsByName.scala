package com.sparkscala.basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average number of friends by name in a social network. */
object FriendsByName {
  
  def parseLines(line: String) = {
    val fields = line.split(",");
    val name = fields(1).toString;
    val numOfFrieds = fields(3).toInt;
    (name, numOfFrieds);
  }
  
  def main(args: Array[String]) {

    //Get Logger and set the log level
    Logger.getLogger("org").setLevel(Level.ERROR);
    
    //Get the spark context
    val sc = new SparkContext("local[*]","FriendsByName");
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("resources/fakefriends.csv");
    
    // Use our parseLines function to convert to (name, numOfFriends) tuples
    val rdd = lines.map(parseLines);
    
    //Get Total by name - Covert (name, numOfFriends) = > (name, (numberOfFrieds, 1)) => (name, (TotalOfAllFriendsOfEveryone, totalNumberOfPeople))
    val totalsByName = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2));
    
    //Find the average age
    val averageAgeByName = totalsByName.mapValues(x => x._1 / x._2);
    
    // Collect the results from the RDD (This kicks off computing the DAG(directed acyclic graph) and actually executes the job)
    val results = averageAgeByName.collect();
    
    //Sort By name and print
    results.sorted.foreach(println);
    
  }

}
  
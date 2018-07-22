package com.sparkscala.basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

/** Compute the Minimum temparature by location */
object MinTemperatures {
  
  def parseLines(line: String) = {
    val fields = line.split(",");
    val sationId = fields(0);
    val entryType = fields(2);
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f;
    (sationId, entryType, temperature);
  }
  
  def main(args: Array[String]) {

    //Get Logger and set the log level
    Logger.getLogger("org").setLevel(Level.ERROR);
    
    //Get the spark context
    val sc = new SparkContext("local[*]","MinTemperatures");
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("resources/1800.csv");
    
    // Use our parseLines function to convert to (sationId, entryType, temperature) tuples
    val rdd = lines.map(parseLines);
    
    //filter the lines and return only line with TMIN field type
    val minTempratures = rdd.filter(x => x._2 == "TMIN");
    
    //Apply map on Min tempratures to return the list of (sationId, temperature)
    val sationTemprature = minTempratures.map(x => (x._1, x._3.toFloat));
    
    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = sationTemprature.reduceByKey((x,y) => min(x,y));
    
    //Collect the results
    val results = minTempsByStation.collect().sorted;
    
    for(result <- results) {
      val stationId = result._1;
      val temp = result._2;
      val formattedTemp = f"$temp%.2f F"
      println(s"$stationId minimum temperature: $formattedTemp") 
    }
    
  }

}
  
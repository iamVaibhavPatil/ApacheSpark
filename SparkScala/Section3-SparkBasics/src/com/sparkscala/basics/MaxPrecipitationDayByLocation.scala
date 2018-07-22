package com.sparkscala.basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Compute the What day had most Precipitation by location */
object MaxPrecipitationDayByLocation {
  
  def parseLines(line: String) = {
    val fields = line.split(",");
    val sationId = fields(0);
    val date = fields(1);
    val entryType = fields(2);
    val precipitation = fields(3).toInt;
    (sationId, date, entryType, precipitation);
  }
  
  def main(args: Array[String]) {

    //Get Logger and set the log level
    Logger.getLogger("org").setLevel(Level.ERROR);
    
    //Get the spark context
    val sc = new SparkContext("local[*]","MaxTemperatures");
    
    // Load each line of the source data into an RDD
    val lines = sc.textFile("resources/1800.csv");
    
    // Use our parseLines function to convert to (sationId, date, entryType, precipitation) tuples
    val rdd = lines.map(parseLines);
    
    //filter the lines and return only line with PRCP field type
    val precipitations = rdd.filter(x => x._3 == "PRCP");
    
    //Apply map on Max Precipitation to return the list of (sationId, precipitation)
    val sationDatePrecipitations = precipitations.map(x => (x._1, x._4));

    // Reduce by stationID retaining the maximum temperature found
    val maxPrecipitationByStation = sationDatePrecipitations.reduceByKey((x, y)=> max(x, y));
    
    //Collect the results
    val results = maxPrecipitationByStation.collect().sorted;
    
    for(result <- results) {
      val stationId = result._1;
      val precipitation = result._2;
      println(s"$stationId max precipitation $precipitation");
    }
    
  }

}
  
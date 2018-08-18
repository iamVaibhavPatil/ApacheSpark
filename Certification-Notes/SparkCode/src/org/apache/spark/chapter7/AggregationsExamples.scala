package org.apache.spark.chapter7

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object AggregationsExamples {

  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("AggregationsExamples")
      .master("local[*]")
      .getOrCreate()
    
    // Change the default shuffle partition from 200 to 5
    spark.conf.set("spark.sql.shuffle.partition", "5")      
      
    /* Read Data on purchases, repartition the data to have far fewer partitions(as it is small volume of data stored in lot of small files) 
     * and cache the results for rapid access
     * */  
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\retail-data\\all\\*.csv")
      .coalesce(5)
      
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df.show(false)
    
    /* Use count to get an idea of total size of dataset, but we can also use it to cache an entire DataFrame in memory
     * Here Count is eagerly evaluated.
     *  */
    print(df.count())
    
    
    //**** Aggregation Functions ****
    
  }
}
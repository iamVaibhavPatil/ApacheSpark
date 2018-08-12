package org.apache.spark.chapter6

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object DataTypesExample {
  
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR) 
    
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("SchemaExample")
      .master("local[*]")
      .getOrCreate()
      
    // Change the default shuffle partition from 200 to 5
    spark.conf.set("spark.sql.shuffle.partition", "5")    
    
    // DataFrameStatFunctions - Hold variety of statistically related function 
    // DataFrameNaFunctions - refers function that are relevant when working with null data.
    
    // Reading the data
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\retail-data\\by-day\\2010-12-01.csv")

    // Print the Schema, Create Temporary table and show all the data
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
    df.show()
    
    //*** - Converting to Spark type - using lit
    import org.apache.spark.sql.functions.lit
    df.select(lit(5), lit("five"), lit(5.0)).show()
    
  }
}
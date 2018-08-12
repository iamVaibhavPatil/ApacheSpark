package org.apache.spark.chapter5

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata


object SchemaExample {

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
    
    val myManualSchema = StructType(Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
        ))
    
    val df = spark.read.format("json").schema(myManualSchema).load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\flight-data\\json\\2015-summary.json") 
    
    // Select and SelectExpr
    df.printSchema()  
    
    df.select("DEST_COUNTRY_NAME").show(2)
    
    df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

    df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
  
    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
   
    // Converting to Spark Types(literals)
    import org.apache.spark.sql.functions.{lit, expr}
    df.select(expr("*"), lit(1).as("One")).show(2)
  }
}
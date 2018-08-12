package org.apache.spark.chapter3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{window, column, col, desc}
import org.apache.log4j._

object StructuredStreaming {

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)      
    
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("StaticDataFrame")
      .master("local[*]")
      .getOrCreate()
      
    // Change the default shuffle partition from 200 to 5
    spark.conf.set("spark.sql.shuffle.partition", "5")
    
    // Read the data from CSV file
    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\retail-data\\by-day\\*.csv")
      
    // Create a schema
    val staticSchema = staticDataFrame.schema
    
    // Create Streaming DataFrame and Read the data from CSV file
    val streamingDataFrame = spark.readStream.format("csv")
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .option("header", "true")
      .load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\retail-data\\by-day\\*.csv") 
      
    // Check if DataFrame is Streaming
    println("Is DataFrame Streaming", streamingDataFrame.isStreaming)
    
    // Read the time series data 1 day at a time and calculate purchase by hour
    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerID",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(col("CustomerID"), window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")
      
    // Call Streaming Action
    purchaseByCustomerPerHour.writeStream
      .format("memory")  // memory = store in-memory table
      .queryName("customer_purchases")  // the name of the in-memory table
      .outputMode("complete")  // complete - All the counts should be in table
      .start()
    
  }
}
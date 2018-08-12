package org.apache.spark.chapter3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{window, column, col, desc}
import org.apache.log4j._

object StaticDataFrame {
  
  def main(args: Array[String]) {
   
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
      
    // Create a temporary table
    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    
    // Read the time series data 1 day at a time and show first 5 rows
    staticDataFrame
      .selectExpr(
        "CustomerID",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(col("CustomerID"), window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")
      .show(5)
    
  }
}
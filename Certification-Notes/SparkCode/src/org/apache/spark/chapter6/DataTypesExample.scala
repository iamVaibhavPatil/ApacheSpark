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
    
    //*** - Working with Boolean
    import org.apache.spark.sql.functions.col
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)
      
    // Use -> === or =!=
    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)
    
   // Use predicate as an expression
   df.where("InvoiceNo = 536365").show(5, false)
   
   // Use of does not equal to(<>)
   df.where("InvoiceNo <> 536365").show(5, false)
   
   // and - We can either specify and statement as a single statement or chained them to have clear code. Spark flatten them and executes them at once
   // or - statement needs to be specified in single statement
   val priceFilter = col("UnitPrice") > 600
   val descriptionFilter = col("Description").contains("POSTAGE")
   df.where(col("StockCode").isin("DOT"))
     .where(priceFilter.or(descriptionFilter))
     .show()
     
   // Add Boolean Column to DataFrame using Filters
   val DOTCodeFilter = col("StockCode") === "DOT"
   df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
     .where("isExpensive")
     .select("unitPrice", "isExpensive")
     .show(5)
     
   // eqNullSafe -> if we have null data, we can use eqNullSafe instead of equalTo
   df.where(col("Description").eqNullSafe("hello")).show()
   
   // Using DataFrame API vs Spark SQL - Both will give same performance
   import org.apache.spark.sql.functions.{expr, col, not}
   
   // With DataFrame
   df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
     .filter("isExpensive")
     .select("Description", "UnitPrice")
     .show(5)
     
   // With Spark SQL
   df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
     .filter("isExpensive")
     .select("Description", "UnitPrice")
     .show(5)   
   
     
     
   //*** - Working with Numbers - Second most important task after filtering is counting things
     
   // pow - function that raises a column to expressed power
   import org.apache.spark.sql.functions.{pow}
   val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
   df.select(col("CustomerID"), col("Quantity"), fabricatedQuantity.alias("realQuantity")).show(2)
    
   //SQL Expression
   df.selectExpr(
     "CustomerID",
     "(POWER((Quantity * UnitPrice), 2.0) +5) as realQuantity"
   ).show(2)
   
   // Rounding - We can cast to int, which will do the rounding. But we can use more precise functions
   // round --> Round up
   // bround --> round down
   import org.apache.spark.sql.functions.{round, bround, lit}
   df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
   
   // Round up and Round Down
   df.select(round(lit(2.5)), bround(lit(2.5))).show(2)
   
   
   
  }
}
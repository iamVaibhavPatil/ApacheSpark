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
    
    println("---Printing Schema---")
    df.printSchema()  
    
    // Select and SelectExpr
    println("---Selecting Data from Table---")
    df.select("DEST_COUNTRY_NAME").show(2)
    
    df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

    df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)
  
    println("---Getting Average---")
    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
   
    // Converting to Spark Types(literals)
    import org.apache.spark.sql.functions.{lit, expr, col}
    df.select(expr("*"), lit(1).as("One")).show(2)
    
    // Add Column to DataFrame - withColumn method on DataFrame
    df.withColumn("numberOne", lit(1)).show(2)
    
    // Add Column values based on expression while adding new Column to DataFrame
    df.withColumn("numberOne", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
    
    // Rename the new Column by using withColumn and expression
    df.withColumn("dest", expr("DEST_COUNTRY_NAME")).show(2)
    
    // Renaming an existing column using withColumnRenamed method to a new name
    df.withColumnRenamed("DEST_COUNTRY_NAME", "Destination").show(2)
    
    // Reserved Characters and keywords - escape while column name
    
    // Don't need to escape as first argument is just a string
    val dfWithLongColumnName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
    dfWithLongColumnName.show(2)
    
    // Escape in below as we are referencing Column in the expression
    dfWithLongColumnName.selectExpr(
      "`This Long Column-Name`",
      "`This Long Column-Name` as `new col`"
    ).show(2)
    
    ///**** WE ONLY NEED TO ESCAPE EXPRESSIONS THAT USES RESERVED CHARACTERS or KEYWORDS***
    
    // Both below will result in same output
    import org.apache.spark.sql.functions.{col}
    dfWithLongColumnName.select(col("This Long Column-Name")).columns

    dfWithLongColumnName.selectExpr("`This Long Column-Name`").columns
    
    
    //---Case Sensitive-- By Default Spark is case insensitive. We can change is using below configuration setting
    //spark.conf.set("spark.sql.caseSensitive", "true")
    
    //*** Remove Column from DataFrame - drop method. Multiple columns can also be dropped
    df.drop("ORIGIN_COUNTRY_NAME").show(5)
    df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(5)
    
    //*** Cast column to another type - Changing Column type from int to long as below
    df.withColumn("count2", col("count").cast("long")).show(2)
    
    //*** Filtering rows - Filter out the rows with an expression that is equal to false
    // Create expression as String or build expression by using a set of column manipulations
    // where / filter - Both are same
    df.filter(expr("ORIGIN_COUNTRY_NAME == 'United States'")).show(5)
    df.where(expr("ORIGIN_COUNTRY_NAME == 'United States'")).show(5)
    
    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)
    
    // Chaining multiple AND filter
    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)
    
    
    //*** Unique Rows - using distinct method
    println(df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count())
    
    //*** Random Samples
    
    
  }
}
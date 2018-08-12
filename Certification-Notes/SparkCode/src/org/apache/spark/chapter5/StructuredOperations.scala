package org.apache.spark.chapter5

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata


object StructuredOperations {

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
    
    //*** Random Samples - Extracting a fraction of records from a DataFrame
    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    val sampleDataFrame = df.sample(withReplacement, fraction, seed)
    sampleDataFrame.show(5)
    
    //*** Random Splits
    println("---Random Split---")
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    println("Original DataFrame rows: ", df.count())
    println("First DataFrame rows: ", dataFrames(0).count())
    println("Second DataFrame rows: ", dataFrames(1).count())
    
    
    
    //*** Concatenating and Appending Rows(Union)
    import org.apache.spark.sql.Row
    val schema = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country2", "Other Country2", 5L)
    )
    val parallelizeRows = spark.sparkContext.parallelize(newRows)
    val newDf = spark.createDataFrame(parallelizeRows, schema)
    df.union(newDf)
      .where("count == 5")
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show()
    
    
   //*** Sorting Rows - sort() or orderBy() - Default direction is ascending, We can use asc or desc function to specify direction
   println("---Sorting Rows---")
   df.sort("count").show(5)
   df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
   df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
   
   println("---Sorting Rows with directions---")
   import org.apache.spark.sql.functions.{asc, desc, asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last}
   df.orderBy(expr("count desc")).show(2)
   df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
   
   // For more optimization sort within each partition before another set of transformation - sortWithinPartitions
   val sortedDf = spark.read.format("json")
     .schema(myManualSchema)
     .load("D:\\Github\\ApacheSpark\\Certification-Notes\\data\\flight-data\\json\\*-summary.json")
     .sortWithinPartitions("count")
   sortedDf.show(20)
   
   
   //*** Limit
   df.limit(10).show()
   
   
   //*** Repartition and Coalesce
   println(df.rdd.getNumPartitions)
   
   // Repartition into 5 partition - Repartition will incur a full shuffle of the data, regardless of whether one is necessary
   df.repartition(5)
   println(df.rdd.getNumPartitions)
   
   // Coalesce - will not incur full shuffle and will try to combine partitions. 
   // Below operation will shuffle data into 5 partition based on destination country and then Coalesce(combine) them
   df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
  }
}
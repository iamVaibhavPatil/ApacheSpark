package org.apache.spark.chapter3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{window, column, col, desc}
import org.apache.log4j._

object MachineLearning {

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
      
      
    // Convert the data into numerical format which will understood by Machine Learning Models
    import org.apache.spark.sql.functions.date_format
    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
      .coalesce(5)
      
    // Divide the data to create training data and test data
    val trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
    val testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")
    
    // Check the count
    trainDataFrame.count()
    testDataFrame.count()

    // ML transformation which converts the string to index. It can assign the index to days of week like Monday-1, Friday-5
    import org.apache.spark.ml.feature.StringIndexer
    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")
  
    // Encode the values to column
    import org.apache.spark.ml.feature.OneHotEncoder
    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    // All of these will result in set of columns, that we will assemble into vector. All MLLib algorithms in Spark takes Vector as input
    import org.apache.spark.ml.feature.VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")
  
    // Ste up pipeline, so if any future data we need to transform will go through this pipeline
    import org.apache.spark.ml.Pipeline
    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))
  
    val fittedPipeline = transformationPipeline.fit(trainDataFrame)
    val transformedTraining = fittedPipeline.transform(trainDataFrame)
  
    transformedTraining.cache()

    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)

    val kmModel = kmeans.fit(transformedTraining)
    kmModel.computeCost(transformedTraining)
    
    val transformedTest = fittedPipeline.transform(testDataFrame)
    kmModel.computeCost(transformedTest)  
  }
}
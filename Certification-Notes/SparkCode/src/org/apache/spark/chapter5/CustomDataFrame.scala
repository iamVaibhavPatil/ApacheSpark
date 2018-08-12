package org.apache.spark.chapter5

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

object CustomDataFrame {

  def main(args: Array[String]) {
   
    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("SchemaExample")
      .master("local[*]")
      .getOrCreate()
      
    // Change the default shuffle partition from 200 to 5
    spark.conf.set("spark.sql.shuffle.partition", "5")
    
    val myManualSchema = StructType(Array(
        StructField("FIRST_NAME", StringType, true),
        StructField("LAST_NAME", StringType, true),
        StructField("EMP_ID", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
        ))
        
    val myRows = Seq(Row("Vaibhav",null,1571L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDF = spark.createDataFrame(myRDD, myManualSchema)
    myDF.show()
  }  
}
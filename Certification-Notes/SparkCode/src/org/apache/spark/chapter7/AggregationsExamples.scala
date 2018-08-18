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
     * Here Count is action on DataFrame as opposed to transformation and it is eagerly evaluated.
     *  */
    print(df.count())
    
    
    //**** Aggregation Functions ****
    /* Aggregations are available as function in org.apache.spark.sql.functions package. 
     * There are some available on DataFrame in .stat
     * */
    
    // count function - Here count will perform transformation instead of action
    // count(col("column")) - Count number of rows for column - For individual column count, Spark will not count null values.
    // count("*") - Count number of rows for all column. For all column count, Spark will count null values(row containing null values)
    // count(1) - Count every row as literal one
    import org.apache.spark.sql.functions.{count, col}
    df.select(count("StockCode")).show()
    df.select(count("*")).show()
 
    // countDistinct - Count Distinct Values
    import org.apache.spark.sql.functions.countDistinct
    df.select(countDistinct("StockCode")).show()

    /* approx_count_distinct - For large DataSet exact distinct count is irrelevant. Approximation to certain degree of accuracy works
     * just fine and for that, we can use approx_count_distinct function. This is much much faster than countDistinct
     * Second Parameter is - Maximum Estimation Error allowed.
     * */ 
    import org.apache.spark.sql.functions.approx_count_distinct
    df.select(approx_count_distinct("StockCode", 0.1)).show()
    
    // first and last - Get first and last row from DataFrame. Based on rows, not on values
    import org.apache.spark.sql.functions.{first, last}
    df.select(first("StockCode"), last("StockCode")).show()
    
    // min and max - Extract minimum and maximum value from DataFrame
    import org.apache.spark.sql.functions.{min, max}
    df.select(min("Quantity"), max("Quantity")).show()
    
    // sum - add all values in a row
    import org.apache.spark.sql.functions.sum
    df.select(sum("Quantity")).show()
    
    
    
  }
}
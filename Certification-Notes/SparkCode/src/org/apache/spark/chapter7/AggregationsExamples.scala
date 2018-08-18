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


    /* **** Aggregation Functions ****
     * 
     * Aggregations are available as function in org.apache.spark.sql.functions package. 
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
    
    // sumDistinct - sum a distinct set of values
    import org.apache.spark.sql.functions.sumDistinct
    df.select(sumDistinct("Quantity")).show()
    
    /* avg(mean) - We can calculate avg by dividing sum by count, but Spark provides an easier way to get that value by avg or mean.
     * Below will return same result in all 3 columns.
     * */
    import org.apache.spark.sql.functions.{avg, mean, expr}
    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchase"),
      avg("Quantity").alias("avg_purchases"),
      mean("Quantity").alias("mean_purchases")
    ).selectExpr(
      "total_purchase / total_transactions",
      "avg_purchases",
      "mean_purchases"
    ).show()
    
    /* Variance and Standard Deviation - Calculating mean brings up the question about the variance and deviation as they both 
     * are measures of spread of the data around the mean.
     * Variance - Average of the squared differences from the mean
     * Standard Deviation - Square root of the variance
     * 
     * 1) Sample Standard Deviation - Default for Spark, if we dont specify and use variance or stddev function
     * 2) Population Standard Deviation
     * */ 
    import org.apache.spark.sql.functions.{var_samp,stddev_samp}
    import org.apache.spark.sql.functions.{var_pop,stddev_pop}
    df.select(var_pop("Quantity"), var_samp("Quantity"), stddev_pop("Quantity"), stddev_samp("Quantity")).show()
    
    /* skewness and kurtosis - Measurements of extreme point in the data.
     * skewness - Asymmetry of values in data around the mean
     * kurtosis - measures of the tail of data.
     * 
     * These both are used when modeling data as a probability distribution of a random variable.
     * */
    import org.apache.spark.sql.functions.{skewness, kurtosis}
    df.select(skewness("Quantity"), kurtosis("Quantity")).show()
    
    /* Covariance and Correlation - Campres interaction of values in two different columns
     * 
     * Correlation - Measures the Pearson Correlation coefficient between values in 2 columns, which is scaled between -1 and +1
     * Covariance - Variance between 2 columns. Has both Sample and Population method.
     * */
    import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
    df.select(
        corr("InvoiceNo", "Quantity"),
        covar_samp("InvoiceNo", "Quantity"),
        covar_pop("InvoiceNo", "Quantity")
    ).show()
    
    /* Aggregating to Complex Types - We can also perform aggregation on complex type.
     * We can collect list of values present in a given column or only unique values collecting to a set.
     * We can pass the collection values in UDFs or can use them later on in pipeline.
     * */
    // Commented, as it takes lot of time to process
    import org.apache.spark.sql.functions.{collect_list, collect_set}
    //df.agg(collect_list("Country"), collect_set("Country")).show(false)
    
    
    /* **** Grouping ****
     * So far we have performed only DataFrame level aggregations. A common task is to perform calculations based on groups in the data.
     * This is done on categorical data on which we group our data on one column and perform some calculations on other columns that end up in that group.
     * 
     * Grouping is done in two phases - 1) Specify the columns on which we would like to group, this returns RelationalGroupedDataSet
     * 2) then, we specify the aggregation(s), this returns a DataFrame
     * */
    df.groupBy("InvoiceNo", "CustomerId").count().show()
    
    
    /* Grouping with Expressions - We can sepcify aggregation function within expression.
     * Instead of passing expression in select statement, we need to specify in agg, which makes it possible to pass any arbitrary expressions that have some aggregation.
     * */
    import org.apache.spark.sql.functions.expr
    df.groupBy("InvoiceNo").agg(
      count("Quantity").alias("Quan"),
      expr("count(Quantity)")
    ).show()
    
    /* Grouping with Maps - We can also specify transformations as a series of Maps for which the key is the column, 
     * and the value is the aggregation function that we would like to perform on column.
     * */
    df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show()
    
    
    /* **** Window Functions ****
     * Window can be used to do aggregation on specific window of data. A group-by takes data, and every row can go only into one grouping.
     * A window function calculates return value for every input row of a table based on a group of rows, called a frame. Each row can fall into one or more frames.
     * Spark defines 3 window functions - ranking functions, analytic functions, and aggregate functions.
     */
    import org.apache.spark.sql.functions.{col, to_date}
    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    dfWithDate.show(false)
    
    /* First step to window function is to create a window specification.
     * partitionBy - Defines, how we will be breaking our group
     * orderBy - Defines, ordering within given partition
     * rowsBetween - this is frame specification states which rows will be included in the frame based on its reference to the current row.
     * */
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)  // Include previous rows up to the current row
      
    /* Now we can use an aggregation function to learn more about specific customers.
     * For example - Finding maximum purchase quantity over the time.
     * In addition to this we indicate the window specification that defines to which frames of data this function will apply
     * */
    import org.apache.spark.sql.functions.max
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec) // Returns column or expression which can be used in DataFrame select
    
    /* Now we need to create purchase quantity rank which tells us which date had the maximum purchase quantity for every customer.
     * dense_rank(), rank() function gives us the ranking over window.
     * This also returns a column which can be used in the select statement of DataFrame
     * */
    import org.apache.spark.sql.functions.{dense_rank, rank}
    val purchaseDenseRank =  dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    // Perform Select to view the calculated purchase over the window time
    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("QuantityRank"),
        purchaseDenseRank.alias("QuantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")
      ).show()
    
      
    /* **** Grouping Sets ****
     * group-by is used to aggregate on a set of columns. But when we want aggregation across multiple groups. We achieve this by using grouping sets.
     * Grouping set are low level tool for combining set of aggregations together.
     * 
     * Below we will get the total quantity of all stock codes and customers using grouping sets. 
     * Grouping set depend on null values for aggregation levels. If you do not filter out null values, you will get incorrect results.
     * This applies to cubes, rollups, and grouping sets
     * 
     * GROUPING SET - Only available in SQL. To perform same in DataFrame we need to use rollup and cube operators
     * */
    val dfNoNull = dfWithDate.drop()
    dfNoNull.createOrReplaceTempView("dfNoNull")
    
    // Grouping two columns
    spark.sql("""
      SELECT CustomerId, StockCode, sum(Quantity) FROM dfNoNull
      GROUP BY CustomerId, StockCode
      ORDER BY CustomerId DESC, StockCode DESC
      """).show()
    
    // Grouping set
    spark.sql("""
      SELECT CustomerId, StockCode, sum(Quantity) FROM dfNoNull
      GROUP BY CustomerId, StockCode GROUPING SETS((CustomerId, StockCode))
      ORDER BY CustomerId DESC, StockCode DESC
      """).show()  
   
    /* Rollups - Rollup is a multidimensional aggregation that performs variety of group by style calculations for us.
     * 
     * Below rollup will looks across time(with new Date column) and space(with country column) and creates a new DataFrame
     * that includes the grand total over all dates, the grand total for each date in the DataFrame, and the subtotal for each country
     * on each date in the DataFrame
     * 
     * null in both columns specifies the grand total across both of these columns
     * */
    val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rolledUpDF.show()
    
    /* Cube - Cube takes rollups to deeper level. This is greate for creating summary table which can be used later.
     * It answers all the aggregation question across all columns
     * */
    val cubDF = dfNoNull.cube("Date", "Country").agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    cubDF.show()
  }
}
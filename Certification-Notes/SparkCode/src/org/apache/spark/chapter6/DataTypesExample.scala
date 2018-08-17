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
   
   // Find Pearson Correlation Coefficient for 2 columns if cheaper things are typically bought in greater quantities
   // We can either use stat functions on DataFrame or use corr function
   import org.apache.spark.sql.functions.{corr}
   df.stat.corr("Quantity", "UnitPrice")
   df.select(corr("Quantity", "UnitPrice")).show()
   
   // Computing summary statistics for numeric columns - describe method on DataFrame.
   // It will list count, mean, standard deviation, min and max on all numeric column
   df.describe().show()
   
   // We can also use individual functions on columns to extract these values from DataFrame
   import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}
   
   // Stats function
   val quantileProbs = Array(0.5)
   val relError = 0.05
   df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
   
   // Cross tabulation or frequent item pairs
   df.stat.crosstab("StockCode", "Quantity").show()
   
   // Get the frequent item pairs
   df.stat.freqItems(Seq("StockCode", "Quantity")).show()

   // add unique increasing id - starting with 0
   import org.apache.spark.sql.functions.{monotonically_increasing_id}
   df.select(monotonically_increasing_id()).show(10)
   
   
   
   //*** - Working with Strings -
   
   // Initialize Every word in string
   import org.apache.spark.sql.functions.{initcap}
   df.select(initcap(col("Description"))).show(2)
   
   // Uppercase and lowercase
   import org.apache.spark.sql.functions.{upper,lower}
   df.select(col("Description"), lower(col("Description")), upper(col("Description"))).show(5)
   
   //lpad,ltrim,rpad,rtrim,trim
   import org.apache.spark.sql.functions.{lpad,ltrim,rpad,rtrim,trim}
   df.select(
     ltrim(lit("  HELLO    ")).as("ltrim"),
     rtrim(lit("  HELLO    ")).as("rtrim"),
     trim(lit("   HELLO    ")).as("trim"),       
     lpad(lit("HELLO") ,3, "*").as("lp"),
     rpad(lit("HELLO"), 10,"*").as("rp")
   ).show(2)
   
   // Regular Expression
   // regexp_extract - Extract Values
   // regexp_replace - Replace Values
   
   // Regular expression to replace color names with 'COLOR' string
   import org.apache.spark.sql.functions.{regexp_replace}
   val simpleColors = Seq("BLACK","WHITE","GREEN","RED","BLUE")
   val regexString = simpleColors.map(x => x.toUpperCase()).mkString("|")
   df.select(
     col("Description"),
     regexp_replace(col("Description"), regexString, "COLOR").as("COLOR_CLEAN")
   ).show(5)
   
   // translate - Character level replacement - Will replace all instances of a character with the indexed character
   import org.apache.spark.sql.functions.translate
   df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(5)
   
   // Extracting Data - Pulling out first mentioned color
   import org.apache.spark.sql.functions.{regexp_extract}
   val regexString2 = simpleColors.map(_.toUpperCase()).mkString("(", "|", ")")
   df.select(
     regexp_extract(col("Description"), regexString2, 1).as("color_clean"),
     col("Description")
   ).show(5)
   
   // Contains - To check if the value exists - This is for Scala
   // in Python, we can use instr function for contains checking
   val containsBlack = col("Description").contains("BLACK")
   val containsWhite = col("Description").contains("WHITE")
   df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
     .where("hasSimpleColor")
     .select("Description").show(5, false)
   
   //Dynamic number of arguments using - varargs
   val simpleColors3 = Seq("black", "white", "red", "green", "blue")
   val selectedColumns = simpleColors3.map(color => {
     col("Description").contains(color.toUpperCase()).alias(s"is_$color")
   }):+expr("*")
   
   df.select(selectedColumns:_*)
     .where(col("is_white").or(col("is_red")))
     .show(5, false)
   
     
     
   //*** - Working with Dates and Timestamps
   // spark.conf.sessionLocaleTimeZone - To set the timezone
   // TimestampType - Supports only second level precision. If we are going to be working with milliseconds or microseconds, we need to use longs.
     
   // Create Simple Table
   import org.apache.spark.sql.functions.{current_date, current_timestamp}
   val dateDF = spark.range(10)
     .withColumn("today", current_date())
     .withColumn("now", current_timestamp())  
   dateDF.createOrReplaceTempView("dateTable")
   
   dateDF.printSchema()
   dateDF.show(10, false)
     
   // Add or Subtract date
   import org.apache.spark.sql.functions.{date_add, date_sub}
   dateDF.select(date_add(col("today"), 5), date_sub(col("today"), 5)).show(2)
   
   // datediff - Returns Number of days between 2 dates
   // months_between - number of months between 2 dates
   // to_date - allows to convert a string to a date. optionally with a specified format(Java SimpleDateFormat)
   import org.apache.spark.sql.functions.{datediff, months_between, to_date}

   dateDF.withColumn("week_ago", date_sub(col("today"), 7))
     .select(datediff(col("week_ago"), col("today"))).show(1)
     
   // months_between
   dateDF.select(
     to_date(lit("2016-01-01")).alias("start"),
     to_date(lit("2017-02-01")).alias("end")
   ).select(months_between(col("start"), col("end"))).show(1)
   
   // to_date
   spark.range(5).withColumn("date", lit("2017-01-01"))
     .select(to_date(col("date"))).show(1)
   
   // Date Format with Spark - Spark will return null, if not able to parse the date instead of throwing error.
   // year-month-day to year-day-month
   dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)
   
   // As Spark does not know the format and returns null, We can fix above issue of null, by specifying the Java SimpleDateFormat.
   // to_date - Date format is optional
   // to_timestamp - Date format is required
   
   val dateFormat = "yyyy-dd-MM"
   val cleanDateDF = spark.range(1).select(
     to_date(lit("2017-12-11"), dateFormat).alias("date"),
     to_date(lit("2017-20-12"), dateFormat).alias("date2")
   )
   cleanDateDF.createOrReplaceTempView("dateTable2")
   cleanDateDF.show()
   
   import org.apache.spark.sql.functions.to_timestamp
   cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
   
   
   //*** - Working with Nulls in Data
   
   // Use .na subpackage of DataFrame
   
   // coalesce - select first non-null value from set of columns
   import org.apache.spark.sql.functions.coalesce
   df.select(coalesce(col("Description")), coalesce(col("CustomerId"))).show()
   
   // SQL Functions for Null
   // ifnull - returns second value if the first is null, and default to first
   // nullif - returns null if two values are equal or else returns second if they are not equal
   // nvl - returns second value if first is null, but defaults to first
   // nvl2 - returns second value if the first is not null, otherwise it will return the last specified value(else_value)
   
   spark
     .sql(
         "SELECT ifnull(null, 'return_value'), nullif('value','value'), nvl(null, 'return_value'), nvl2('not_null','return_value','else_value')")
     .show()
   
   // drop - removed rows that contains nulls
   // drop("any") - drops a row if any of the values are null
   // drop("all") - drops a row only if all values are null or NaN for that row
   // Pass to certain columns
   df.na.drop().show(5, false)
   df.na.drop("any").show(5, false)
   df.na.drop("all").show(5, false)
   df.na.drop("all", Seq("StockCode", "InvoiceNo")).show(5, false)
   
   // fill() - fill one or more columns with set of values for null value in the column
   // fill(5:Integer) - Column of type Integer
   // fill(5:Double) - Column of type Double
   // We can also specify scala map for column with values to fill for null
   df.na.fill("All null values becomes this string")
   df.na.fill(5, Seq("StockCode", "InvoiceNo")).show(5)
   
   val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
   df.na.fill(fillColValues).show(5)
   
   // replace - replace all values in certain column according to their current value. New value type should match with original value type
   df.na.replace("Description", Map("" -> "UNKNOWN")).show(5)
   
   
   //*** - Working with Complex Types
   // 3 complex types - structs, arrays, maps
   
   //struct - DataFrames with DataFrame
   df.selectExpr("(Description, InvoiceNo) as complex", "*").show(5, false)
   df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(5, false)
   
   import org.apache.spark.sql.functions.struct
   val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
   complexDF.createOrReplaceTempView("complexDfTable")
   
   //We need to either dot(.) or getField method to get the subfield from complex
   complexDF.select("complex.Description").show()
   complexDF.select(col("complex").getField("Description")).show()
   
   // All values in complex type
   complexDF.select("complex.*")
   
   
   
  }
}
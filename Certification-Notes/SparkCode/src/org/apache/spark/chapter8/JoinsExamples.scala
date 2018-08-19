package org.apache.spark.chapter8

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object JoinsExamples {

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
    
    
    /* **** Join Expression  *****
     * A join brings together two sets of data, the left and the right, by comparing the value of one or more keys of the left and
     * right and evaluating the result of join expression that determines whether Spark should bring together the left set of data
     * with right set of data.
     * 
     * equi-join - Compares whether specified keys in your left and right datasets are equal. If equal, Spark will combine the left and right datasets.
     * We can even use complex types and check whether keys exists within array when you perform join.
     * */
    
    
    /* **** Join Types  *****
     * Join type determines what should be in the result set.
     * 
     * 1) Inner joins - Keep rows with keys that exist in the left and right datasets
     * 2) Outer joins - Keep rows with keys that exist in either left or right datasets
     * 3) Left Outer joins - Keep rows with keys that exist in left dataset
     * 4) Right Outer joins -  Keep rows with keys that exist in right dataset
     * 5) Left Semi joins - keep the rows in left dataset where keys appear in the right dataset
     * 6) Left Anti joins - Keep the rows in left dataset where keys do not appear in the right dataset
     * 7) Natural joins - perform a join by implicitly matching the columns between the two datasets with the same names
     * 8) Cross or Cartesian joins - match every row in the left dataset with every row in right dataset
     * */
    import spark.implicits._   //Without this import toDF will not work in Seq
    
    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
    
    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
    
    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
    .toDF("id", "status")
    
    // Lets register above DataFrames as tables
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    
    
    /* **** Inner Join *****
     * Join graduateProgram and person table to include rows with keys exists in both tables
     * Inner join are the default join, so we just need to specify our left DataFrame and join the right in JOIN expression
     * */
    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpression).show()
    
    // SQL
    spark.sql("SELECT * FROM person JOIN graduateProgram ON person.graduate_program = graduateProgram.id").show()
    
    // We can also specify the join type explicitly
    var joinType = "inner"
    person.join(graduateProgram, joinExpression, joinType).show()
    
    // INNER JOIN SQL
    spark.sql("SELECT * FROM person INNER JOIN graduateProgram ON person.graduate_program = graduateProgram.id").show()
    
    
    /* **** Outer Join *****
     * Keep rows with keys that exist in either left or right datasets. If there is no equivalent row in either the left or
     * the right DataFrame, Spark will insert null.
     * */
    joinType = "outer"
    person.join(graduateProgram, joinExpression, joinType).show()
    
    // OUTER JOIN SQL
    spark.sql("SELECT * FROM person FULL OUTER JOIN graduateProgram ON person.graduate_program = graduateProgram.id").show()
    
    
    /* **** Left Outer Join *****
     * Evaluates the keys in both of the DataFrames and includes all rows from left DataFrame as well as any rows in the right
     * DataFrame that have a match in the left DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null
     * */
    joinType = "left_outer"
    graduateProgram.join(person, joinExpression, joinType).show()

    // LEFT OUTER JOIN SQL
    spark.sql("SELECT * FROM graduateProgram LEFT OUTER JOIN person ON person.graduate_program = graduateProgram.id").show()
    
    
    /* **** Right Outer Join *****
     * Evaluates the keys in both of the DataFrames and includes all rows from right DataFrame as well as any rows in the left
     * DataFrame that have a match in the right DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null
     * */
    joinType = "right_outer"
    person.join(graduateProgram, joinExpression, joinType).show()

    // RIGHT OUTER JOIN SQL
    spark.sql("SELECT * FROM person RIGHT OUTER JOIN graduateProgram ON person.graduate_program = graduateProgram.id").show()    
    
  }
}
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
    
    
    /* **** Left Semi Join *****
     * keep the rows in left dataset where keys appear in the right dataset. Even if the ther are duplicate keys in left, those will be kept.
     * We can think of left semi join as filters on DataFrame.
     * */
    joinType = "left_semi"
    graduateProgram.join(person, joinExpression, joinType).show()
    
    // LEFT SEMI JOIN SQL
    spark.sql("SELECT * FROM graduateProgram LEFT SEMI JOIN person ON person.graduate_program = graduateProgram.id").show()    
     
    // Duplicated Key in the left
    val graduateProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
    
    graduateProgram2.createOrReplaceTempView("graduateProgram2")
    graduateProgram2.join(person, joinExpression, joinType).show()
    
    
    /* **** Left Anti Join *****
     * Keep the rows in left dataset where keys do not appear in the right dataset.
     * We can think of anti joins as a NOT IN sql filter
     * */
    joinType = "left_anti"
    graduateProgram.join(person, joinExpression, joinType).show()
    
    // LEFT ANI JOIN SQL
    spark.sql("SELECT * FROM graduateProgram LEFT ANTI JOIN person ON person.graduate_program = graduateProgram.id").show()
    
    
    /* **** Natural Join *****
     * Perform a join by implicitly matching the columns between the two datasets with the same names.
     * Left, right, and outer natural joins are all supported. We should always use this join with caution,
     * because same column name in different DateFrames means different things.
     * Below query will give wrong results, as id columns has different meaning in both DataFrames.
     * */
    spark.sql("SELECT * FROM graduateProgram NATURAL JOIN person").show()
    
    
    /* **** Cross Join *****
     * Match every row in the left dataset with every row in right dataset. Cross-joins in simplest terms are inner joins that do
     * not specify a predicate. Cross-join will match every single row in the left DataFrame to every single row in the right DataFrame.
     * This will cause absolute explosion in the number of rows contained in the resulting DataFrame.
     * If we have 1000 rows in each DataFrame, the cross join of these will result in 1000000 rows.
     * */
    joinType = "cross"
    graduateProgram.join(person, joinExpression, joinType).show()
    
    // CROSS JOIN SQL
    spark.sql("SELECT * FROM graduateProgram CROSS JOIN person ON person.graduate_program = graduateProgram.id").show()
    
    // We can also call out crossJoin on DataFrame itself
    person.crossJoin(graduateProgram).show()
    
    /* Cross joins are dangerous, so we should use only when necessary. We can set the session level configuration
     * spark.sql.crossJoin.enable = true in order to allow cross join without warning or without Spark trying to perform another join for you.
     * */
    
    
    /* **** Challenges When Using Joins ***** */
    
    /* * Joins on Complex Types
     * Any expression is a valid join expression, assuming that it returns a Boolean.
     * */
    import org.apache.spark.sql.functions.expr
    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
    
    /* * Handling Duplicate Column Names
     * Sometimes we might have duplicate columns in the result dataframe, if both dataframes which we are joining have same column names.
     * In a DataFrame, each column has a unique ID within Spark's SQl Engine, Catalyst. This unique ID is purely internal and not something,
     * that we can directly reference.
     * */
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
    person.join(gradProgramDupe, joinExpr).show()
    
    // Now When we try to access graduate_program column as below, we will get ambiguity error
    //person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
      
    /* To solve above problem, we can do 3 things
     * 
     * 1) Different Join Expression - Write String or Sequence join expression instead of Boolean expression
     * 2) Drop column after the join
     * 3) Rename column before join
     *  */
    
    // Different Join Expression
    person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()
    
    // Drop column after the join
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).select("graduate_program").show()
    
    // Rename column before join
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr3 = gradProgram3.col("grad_id") === person.col("graduate_program")
    person.join(gradProgram3, joinExpr3).show()
    
    
    /* **** How Spark Performs Joins ***** \
     * To understand how Spark performs joins, we need to understand two core resources at play 
     * 1) node-to-node communication strategy
     * 2) per node computation strategy.
     * 
     * Spark approaches cluster communication in two different ways during joins.
     * 1) Shuffle join - Which results in all-to-all node communication
     * 2) Broadcast join - Broadcast small table to all driver node to avoid later communication.
     * 
     * The core foundation of simplified view of joins is that Spark will have either big table or small table.
     * 
     * */
    
    /* ** Big Table-to-Big Table
     * When we have to join big table to another big table, we end up with shuffle join.
     * In shuffle join, every node talks to every other node and they share the data according to which node has a certain key or set of keys on which we are joining.
     * These joins are expensive because the network can become congested with traffic., especially if data is not partitioned well.
     * 
     * Refer - joining-2-big-tables.png. Both the DataFrames are large. This means that all the worker node(potentially every partition)
     * will need to communicate with another during the entire process.
     * 
     * An example might be a company that receives billions of messages every day from IoT, and need to identify day-over-day changes that have occurred.
     * The way to do this is by joining deviceId, messageType, and date in one column, and date - 1 day in the other column. 
     * */
    
    /* ** Big Table-to-Small Table
     * When the table is small enough to fit into memory of single worker node, with some breathing room of course, we can optimize the join.
     * This is called as broadcast join which means is that we will replicate our small DataFrame onto every worker node in the cluster.
     * This sounds expensive, however, it prevents us from performing all-to-all communication during the entire join process. Instead, we
     * perform it only once at the begining and them let each individual work node perform work without having to wait or communicate with
     * any other node.
     * 
     * Refer - broadcast-join.png. At the beginning of this join will be a large communication. However, immediately after first,
     * there will be no further communication between nodes.
     * This means that join will be perform on single node individually, making CPU the biggest bottleneck.
     * 
     * For our current set of data, we can see Spark has automatically set this up as a broadcast join. 
     * 
     * == Physical Plan ==
					*BroadcastHashJoin [graduate_program#11], [id#27], Inner, BuildRight
					:- LocalTableScan [id#9, name#10, graduate_program#11, spark_status#12]
					+- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
   				+- LocalTableScan [id#27, degree#28, department#29, school#30]
     * */
    val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr2).explain()
    
    // With DataFrame API, we can also hint the optimizer that we would like to use broadcast join by using correct function on small DataFrame.
    import org.apache.spark.sql.functions.broadcast
    person.join(broadcast(graduateProgram), joinExpr2).explain()
    
    // SQL interface can also provide hint to optimizer with MAPJOIN, BROADCAST and BROADCASTJOIN. These are not enforce, Optimizer can ignore them.
    
    /* ** Little Table-to-little Table - Spark will decide how to join them.
     * */

  }
}
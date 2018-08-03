# Apache Spark - Unified analytics engine for large-scale data processing

## Introduction
Apache Spark is a unified computing engine and set of libraries for parallel data processing on computer clusters. 

1. Spark support multiple widely used programming languages like **Python,Java, Scala, R**.  
2. Spark includes support for libraries for diverse tasks ranging from SQL to streaming and machine learning.  
3. Spark can run on single computer or cluster of computers which makes it easy system to start with and scale up the big data processing at large scale.  
4. Spark is composed of a number of different components as below

<<Spark Tookit from page 13>>

**Unfied**  
Spark is unified platform which means it is designed to support a wide range of data analytics tasks, ranging from simple data loading and SQL queries to machine learning and streaming computations, over the same computing engine and with a consistent set of APIs.

The main goal behind this is that read world data analytics tasks- whether they are interactive analytics in tool such as Jupyter notebook or traditional production applications which tends to combine many differnt processing types and libraries.

Spark's unified nature makes these tasks both easier and more efficient to write. Spark provides consistent, composable APIs that you can use to build an application out of smaller pieces or out of existing liraries. It also makes it easy for us to write our own analytics libraries on top.

Spark's APIs are also designed to enable high performance by optimizing across different libraries and functions composed together in a user program.

For Example - If we load data using SQL query and then evaluate machine learning model over it using Spark's ML library, then engine can combine these steps into one scan over the data. The combination os general APIs and high performance execution, no matter how we combine them makes Sparks a power platform for interactive and production applications.

Data scientists benefits from unified set of libraries e.g. Python and R when doing modeling and web developers benefits from unified frameworks such as NodeJS or Django. Before Spark, no open source systems tried to provide this type of unified engine for paraller data processing, mean that we had to stitch together an application ouf of multiple APIs and systems.

**Computing Engine**  
Spark strives for unification, but it carefully limits it's scope to a computing engine. By this, we mean that Spark handles loading data from storage systems and performing a computation on it, but it's not permanent storage itself.

We can use Spark with a wide variety of persistent storage systems, 

- Azure Storage  
- Amazon S3  
- Distributed file systems such as Apache Hadoop  
- Key-value stores such as Apache Cassandra  
- Message buses such as Apache Kafka  

Key motivation here is that most of the data already resides in a mix of storage systems. Data is expensive to move so Spark focus on performing the computations over the data, no matter where it resides.

Spark's focus on computation makes it different from the earlier big data software platforms such as Apache Haddop. Hadoop included both storage systems(the Hadoop file system, designed for lo cost storage over clusters of commodity servers) and computing system(MapReduce), which were closely integrated together. However, this choice makes it difficult to run one of the systems without the other. More important, this choice also makes it challenge to write applications that access data stored anywhere else.

Sparks runs well on Hadoop storage, but it is used broadly in envrionments for which Haddop architecture does not make sense, such as the public cloud or streaming applications.

**Libraries** 
Sparks supports standard libaries that ship with the engine as well as a wide array of external libraries published as third party packages by open source communities. Sparks includes libraries for SQL and structured data(Spark SQL), machine learning(MLlib), stream processing(Spark Streaming and new Structure Streaming), and graph analytics(GraphX).

There are other open source libraries ffrom connectors for various storage systems to machine learning algorithms. We can check all the spark supported libraries - https://spark-packages.org/


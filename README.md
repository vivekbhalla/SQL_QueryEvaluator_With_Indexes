SQL_QueryEvaluator_With_Indexes
===============================

Uses a pre-computation step to improve the performance of the system by gathering statistics, building indexes, etc

In this project, there is a pre-computation phase with which we can improve the system's performance.
In this additional time the program builds indexes by using the CREATE TABLE statements provided which
defines the schema which has with them the PRIMARY KEY and UNIQUE entries.

#### Indexes

An open-source library is used so that index creation becomes easier. In this case, rather than
building an on-disk indexing system (a number of these are available), a simple-in-memory indexing
system is being used. For this project we will be linking against the [JDBM2 open-source Key/Value store](https://code.google.com/p/jdbm2/). 

This library provides both on-disk HashMap and TreeMap interfaces, supporting both clustered (Primary)
and unclustered (Secondary) indexes, as well as extremely limited support for transactional
access (one transaction at a time).

A short example is available on the [main page](http://jdbm2.googlecode.com/svn/trunk/javadoc/jdbm/package-summary.html), and additional documentation is available through the
project's javadoc. Particularly relevant classes include RecordManager, Primary{,Hash,Tree}Map,
and Secondary{Hash,Tree}Map.

For the pre-computation phase (first phase)
Example invocation

  java -Xmx1024m -cp build:jsqlparser.jar:jdbm2.jar edu.buffalo.cse562.Main --build --data /tmp/data --index /tmp/index tpch-schema.sql

For the actual run (second phase)
Example invocation

  java -Xmx1024m -cp build:jsqlparser.jar:jdbm2.jar edu.buffalo.cse562.Main --data /tmp/data --index /tmp/index tpch-schema.sql tphc07a.sql..

The examples use the following directories and files
  • --build: The build flag indicates that it is the first phase.
  • /tmp/data: Table data stored in '|' separated files. Table names match the names provided in the matching CREATE TABLE with the .dat suffix.
  • /tmp/index: A directory that will persist throughout the entire grading process. Store precomputations here.
  • tpch-schema.sql: A file containing the CREATE TABLE statements for all tables in the TPC-H schema.
  • tphc07a.sql – A sql file containing the Select Query.

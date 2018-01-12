=====================
CrateDB Architecture
=====================

CrateDB is a distributed SQL database. From a high-level perspective, the following components can be found in each node of CrateDB:

.. image:: ../resources/architecture.svg

Components
-------------

SQL Engine
............

CrateDB's heart consists of a SQL Engine which deals which takes care of parsing SQL statements and executing them in the cluster.

The engine is comprised of the following components:

1. Parser: Parses the SQL statement into its components.
2. Analyzer: Semantic processing of the statement including verification, type annotation, and optimization.
3. Planner: Builds an execution plan from the analyzed statement.
4. Executor: Executes the plan in the cluster and collects results.

Input
.....

When you first use CrateDB, you probably want to check out its Web Interface. The Web Interface provides an good overview of your cluster nodes, its health, and a list of your tables. You can also execute queries from it.

CrateDB also has a range of different connectors. For a detailed list, please see:
`https://crate.io/docs/crate/getting-started/en/latest/start-building/index.html`

The Web Interface as well as the connectors all make use of either the REST interface or the Postgres Wire Protocol. The REST interface is HTTP-based wheras the Postgres protocol uses
the TCP protocol underneath. The Postgres protool enables you to use CrateDB for applications
which were built to communicate with Postgres.

Transport
..........

Transport denotes the communication between nodes in a CrateDB cluster. CrateDB uses mainly
Netty and Elasticsearch to transfer data between nodes.

Replication ensures that data is available on multiple nodes and hardware failures can be tolerated.

Storage
........

CrateDB enables you to store your data in talbes like you would in a traditional SQL database.
Additionally, you can define a flexible schema or store JSON objects inside columns. Data can be clustered and partitioned by columns to distribute the data.

To be able to retrieve data efficiently and perform aggregations, CrateDB uses Lucene for indexing and storing data. Lucene itself stores a document with the contents of each row. Retrieval is really efficient because the fields of the document are indexed. Aggregations can also be performed efficiently due to Lucene's column store feature which stores columns separately to quickly perform aggregations with them.

Enterprise
..........

The enterprise version of CrateDB contains additional features.

package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark
import java.sql.DriverManager

object Day31SQLite extends App {
  println("CH9: Data Sources - SQL")

  //SQL Databases
  //SQL datasources are one of the more powerful connectors because there are a variety of systems
  //to which you can connect (as long as that system speaks SQL). For instance you can connect to a
  //MySQL database, a PostgreSQL database, or an Oracle database. You also can connect to
  //SQLite, which is what we’ll do in this example. Of course, databases aren’t just a set of raw files,
  //so there are more options to consider regarding how you connect to the database. Namely you’re
  //going to need to begin considering things like authentication and connectivity (you’ll need to
  //determine whether the network of your Spark cluster is connected to the network of your
  //database system).
  //To avoid the distraction of setting up a database for the purposes of this book, we provide a
  //reference sample that runs on SQLite. We can skip a lot of these details by using SQLite,
  //because it can work with minimal setup on your local machine with the limitation of not being
  //able to work in a distributed setting. If you want to work through these examples in a distributed
  //setting, you’ll want to connect to another kind of database

  val spark = getSpark("Sparky")

  // in Scala
  val driver = "org.sqlite.JDBC"
  val path = "src/resources/flight-data/jdbc/my-sqlite.db"
  val url = s"jdbc:sqlite:${path}" //for other SQL databases you would add username and authentification here
  val tablename = "flight_info"

  //After you have defined the connection properties, you can test your connection to the database
  //itself to ensure that it is functional. This is an excellent troubleshooting technique to confirm that
  //your database is available to (at the very least) the Spark driver. This is much less relevant for
  //SQLite because that is a file on your machine but if you were using something like MySQL, you
  //could test the connection with the following

//just a test of our SQLite connection - just regular Scala code without Spark
//  val connection = DriverManager.getConnection(url)
//  println("SQL connection isClosed:", connection.isClosed())
//  connection.close()

  //If this connection succeeds, you’re good to go. Let’s go ahead and read the DataFrame from the
  //SQL table:

  // in Scala
  val dbDataFrame = spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tablename) //we are loading a single table into a dataframe
    .option("driver", driver) //just a string with the type of database Driver we are using - here jdbc which is most popular
    .load()

  dbDataFrame.printSchema()
  dbDataFrame.describe().show()
  dbDataFrame.show(5, false)

  //As we create this DataFrame, it is no different from any other: you can query it, transform it, and
  //join it without issue. You’ll also notice that there is already a schema, as well. That’s because
  //Spark gathers this information from the table itself and maps the types to Spark data types. Let’s
  //get only the distinct locations to verify that we can query it as expected:

  dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

  //this is optional and could be faster - basically you would ask the database SQL engine to do more work
  //and spark engine to do less

  //Spark can’t translate all of its own functions into the functions available in the SQL database in
  //which you’re working. Therefore, sometimes you’re going to want to pass an entire query into
  //your SQL that will return the results as a DataFrame. Now, this might seem like it’s a bit
  //complicated, but it’s actually quite straightforward. Rather than specifying a table name, you just
  //specify a SQL query. Of course, you do need to specify this in a special way; you must wrap the
  //query in parenthesis and rename it to something—in this case, I just gave it the same table name:

  // in Scala
  val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
AS flight_info"""
  val dbDataFrameFromQuery = spark.read.format("jdbc")
    .option("url", url).option("dbtable", pushdownQuery).option("driver", driver)
    .load()

  //Now when you query this table, you’ll actually be querying the results of that query. We can see
  //this in the explain plan. Spark doesn’t even know about the actual schema of the table, just the
  //one that results from our previous query:

  println(dbDataFrameFromQuery.explain())

  //Writing to SQL Databases
  //Writing out to SQL databases is just as easy as before. You simply specify the URI and write out
  //the data according to the specified write mode that you want. In the following example, we
  //specify overwrite, which overwrites the entire table. We’ll use the CSV DataFrame that we
  //defined earlier in order to do this:

  dbDataFrameFromQuery.show(10)

  // in Scala
  val newPath = "jdbc:sqlite:src/resources/tmp/my-sqlite.db"

  val props = new java.util.Properties
  props.setProperty("driver", "org.sqlite.JDBC")

  dbDataFrameFromQuery //before write you could filter do some aggregartion etc
    .write
    .mode("overwrite")
    .jdbc(newPath, tablename, props)

  //TODO how would you add extra tables to already existing SQL database - probably involves append


}

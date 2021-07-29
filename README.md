# movies
Listing Top 15 Movies By using PySpark

PROJECT REQUIREMENTS
1- Fetch top 15 movies with a minimum of 100 votes. Ranking Formula = (numVotes/averageNumberOfVotes)* averageRating
2- List the title of these top 15 movies

It's been developed with PyCharm IDE, spark3 and hadoop27. Python Interpreter:Python 3.8


Those data files below are not related the requirement, so they have been skipped.
1- title.principals.tsv.gz
2- title.episode.tsv.gz
3- title.crew.tsv.gz
4- title.akas.tsv.gz
5- name.basics.tsv.gz

Only 2 data files below have been used in our transformation codes.
1- title.ratings.tsv.gz
2- title.basics.tsv.gz

The main program is Top15Movies.py
Unit Test program is testutile.py
Functions are in lib\utils.py
Spark session configuration is in spark.conf
Data Files are in Data Folder. title.ratings.tsv.gz and title.basics.tsv.gz files must be copied into Data folder before executing the Python code.
Log4J library,  logger.py and log4j properties have been used for logging. 


Data files are tab delimetered in CSV format. The first line is header and there are different type of characters, such as Japanese, Arabic alphabets. So, the encoding should be UTF-8.
So, while I was reading data, I used those optiorns below:

spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .option("delimiter", "\t") \
        .format("csv") \
        
        
It has been used 2 window function. The first one was to calculate "average number of votes" for entire data set and repeating this value for each row in data frame. The second one is for ranking based on formula. Average should include entire data, but while I am ranking, I eliminated the rows if the number of votes less than 100. At the end, only first 15 top rated data have been fetched.

    top_15_titles_df= title_ratings_df.select("tconst","averageRating","numVotes")\
        .withColumn("averageNumberOfVotes",f.avg("numVotes").over(Window.partitionBy()))\
        .withColumn("rankingRate",f.expr("(numVotes/averageNumberOfVotes)*averageRating"))\
        .where(title_ratings_df.numVotes>=100)\
        .withColumn("rank",f.dense_rank().over(Window.partitionBy().orderBy(f.col("rankingRate").desc()))) \
        .where("rank <=15")


To get the title description, we need to join rating data with title basics as below.

    join_expr = top_15_titles_df.tconst == title_basics_df.tconst
    list_top15_titles = top_15_titles_df.join(title_basics_df, join_expr, "inner") \
        .select("rank",top_15_titles_df.tconst,"primaryTitle","averageRating","numVotes","averageNumberOfVotes","rankingRate")\
        .sort("rank")\
        .show(15)        
        
As a unit test, only row count for each data file have been checked.         


The execution result of the codes is as below:

C:\Users\User\AppData\Local\Programs\Python\Python38-32\python.exe C:/demo/spark-chalenges/MoviesDemo/Top15Movies.py
21/07/29 02:39:00 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/07/29 02:39:03 INFO pyspark-shell: Loading Title Basics into Data Frame has started
[Stage 1:>                                                          (0 + 2) / 2]21/07/29 02:39:15 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
21/07/29 02:39:20 INFO pyspark-shell: Loading Title Ratings into Data Frame has started
21/07/29 02:39:21 INFO pyspark-shell: Fetching Top 15 Movies
21/07/29 02:39:22 INFO pyspark-shell: Listing Top 15 Movies
21/07/29 02:39:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
21/07/29 02:39:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+----+---------+--------------------+-------------+--------+--------------------+------------------+
|rank|   tconst|        primaryTitle|averageRating|numVotes|averageNumberOfVotes|       rankingRate|
+----+---------+--------------------+-------------+--------+--------------------+------------------+
|   1|tt0111161|The Shawshank Red...|          9.3| 2426290|   958.9988271494447|23529.222728114648|
|   2|tt0468569|     The Dark Knight|          9.0| 2382394|   958.9988271494447| 22358.26092064519|
|   3|tt1375666|           Inception|          8.8| 2138333|   958.9988271494447|19621.849232009146|
|   4|tt0944947|     Game of Thrones|          9.2| 1842967|   958.9988271494447| 17680.20556437843|
|   5|tt0137523|          Fight Club|          8.8| 1913481|   958.9988271494447| 17558.55411215844|
|   6|tt0110912|        Pulp Fiction|          8.9| 1882455|   958.9988271494447|  17470.1459748388|
|   7|tt0109830|        Forrest Gump|          8.8| 1874841|   958.9988271494447| 17203.98433545629|
|   8|tt0068646|       The Godfather|          9.2| 1679389|   958.9988271494447|16110.946502327999|
|   9|tt0120737|The Lord of the R...|          8.8| 1709374|   958.9988271494447|15685.620017610167|
|  10|tt0133093|          The Matrix|          8.7| 1728285|   958.9988271494447|15678.934190872442|
|  11|tt0167260|The Lord of the R...|          8.9| 1688142|   958.9988271494447|15666.821871575321|
|  12|tt0903747|        Breaking Bad|          9.4| 1546808|   958.9988271494447| 15161.64023184376|
|  13|tt0816692|        Interstellar|          8.6| 1583993|   958.9988271494447| 14204.75126178353|
|  14|tt0167261|The Lord of the R...|          8.7| 1526381|   958.9988271494447|13847.268968484983|
|  15|tt1345836|The Dark Knight R...|          8.4| 1558893|   958.9988271494447|13654.553925705062|
+----+---------+--------------------+-------------+--------+--------------------+------------------+


Process finished with exit code 0







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
Data Files are in Data Folder
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

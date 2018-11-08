# WordCountAndAverageRateOfMovie
This is a assignment of Hadoop MapReduce. Assignment1b_1 is a modified Word Count with removing stop words. Assignment1b_2 is getting average movie rate.

In assignment1b_1.java:
A modified version of the WordCount algorithm on large files on HDFS taken together using MapReduce.
1)Remove stop words during the map phase,remove words that are less than 5 characters in length, special characters, and convert all words to lowercase.
2)In the reduce phase, you will generate a total count for each key i.e. word and output that to a HDFS file.
 
In assignment1b_2.java:
Read in a large file containing movie ratings and use MapReduce to calculate the average rating for each movie.
1)In the mapper code, read in the ratings.csv file and use the movieId as the key and the rating field as the value.
2)In the reducer code, compute the average rating  for each movieId and store it in the output directory on HDFS.

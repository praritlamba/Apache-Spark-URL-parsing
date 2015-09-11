# Apache-Spark-URL-parsing

*	Extracting :We start by reading the wiki page URL 
*	Cleaning: finding href links using Regular Expression r"<a.*?\s*href=\"(.*?)\".*?>(.*?)</a>" 
*	Loading : Creating an empty list wordlist which will contain all the links .
*	I have generate a base RDD by using a Python list and the sc.parallelize method.
*	UrlRDD = sc.parallelize(wordlist, 4) –Here I have created 4 partitions,so basically 4 parallelisation to compute.
*	Urlpairs is an RDD where each element is a pair tuple (k, v) where k is the key and v is the value. In this , I have created a pair consisting of ('URL', 1) for each Url element in the RDD using the map() transformation with a lambda() function
*	UrlCountsCollected is created using the reduceByKey(). reduceByKey() transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value.
*	uniqueUrl calculates the number of unique URLs 
*	top3UrlsAndCounts sorts the tuple by key and takeOrdered  use to obtain the top 3 Urls 

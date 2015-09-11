
# coding: utf-8

# In[68]:

import urllib2
import re
from operator import add

url=raw_input("Enter wiki page link  ")

wiki=urllib2.urlopen(url)
content=wiki.read()
wordlist=[]
links1 = re.findall(r"<a.*?\s*href=\"(.*?)\".*?>(.*?)</a>", content)
for link in links1:
    wordlist.append(link[0])
#print wordlist
UrlRDD = sc.parallelize(wordlist, 4)

#print UrlRDD.collect()
UrlPairs = UrlRDD.map(lambda x:(x,1))

#print wordPairs.collect()
UrlCountsCollected = (UrlRDD.
                       map(lambda w: (w,1)).reduceByKey(add))

print UrlCountsCollected.collect()

uniqueUrl =UrlCountsCollected.distinct().count()

print "unique url",uniqueUrl

top3UrlsAndCounts = UrlCountsCollected.takeOrdered(3, key=lambda x : -x[1])
print "Top 3 Links in ",url
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top3UrlsAndCounts))


# In[ ]:




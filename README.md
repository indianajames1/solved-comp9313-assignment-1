Download Link: https://assignmentchef.com/product/solved-comp9313-assignment-1
<br>



<h1>Q1. HDFS</h1>

Let <em>N </em>be the number of DataNodes and <em>R </em>be the total number of blocks in the DataNodes.

Assume the replication factor is 5, and <em>k </em>out of <em>N </em>DataNodes have failed simultaneously.

<ol>

 <li>Write down the formula of <em>L<sub>i</sub></em>(<em>k,N</em>) for <em>i </em>∈{1<em>,…,</em>5}, where <em>L<sub>i</sub></em>(<em>k,N</em>) is the number of blocks that have lost <em>i </em></li>

 <li>Let <em>N </em>= 500, <em>R </em>= 20<em>,</em>000<em>,</em>000, and <em>k </em>= 200. Compute the number of blocks that cannot be recovered under this scenario. You need to show both the steps and the final result to get full credit.</li>

</ol>

<h1>Q2. Spark</h1>

Consider the following PySpark code snippet:

raw_data = [(“Joseph”, “Maths”, 83), (“Joseph”, “Physics”, 74),

(“Joseph”, “Chemistry”, 91), (“Joseph”, “Biology”, 82),

(“Jimmy”, “Maths”, 69), (“Jimmy”, “Physics”, 62),

(“Jimmy”, “Chemistry”, 97), (“Jimmy”, “Biology”, 80),

(“Tina”, “Maths”, 78), (“Tina”, “Physics”, 73),

(“Tina”, “Chemistry”, 68), (“Tina”, “Biology”, 87),

(“Thomas”, “Maths”, 87), (“Thomas”, “Physics”, 93),

(“Thomas”, “Chemistry”, 91), (“Thomas”, “Biology”, 74)]

rdd_1 = sc.parallelize(raw_data) rdd_2 = rdd_1.map(lambda x:(x[0], x[2])) rdd_3 = rdd_2.reduceByKey(lambda x, y:max(x, y)) rdd_4 = rdd_2.reduceByKey(lambda x, y:min(x, y)) rdd_5 = rdd_3.join(rdd_4)

rdd_6 = rdd_5.map(lambda x: (x[0], x[1][0]+x[1][1])) rdd_6.collect()

<ol>

 <li>Write down the expected output of the above code snippet.</li>

</ol>

1

<ol start="2">

 <li>List all the stages in the above code snippet.</li>

 <li>What makes the above implmentation inefficient? How would you modifythe code and improve the performance?</li>

</ol>

<h1>Q3: LSH</h1>

Consider a database of <em>N </em>= 1<em>,</em>000<em>,</em>000 images. Each image in the database is pre-processed and represented as a vector <em>o </em>∈ <em>R<sup>d</sup></em>. When a new image comes as a query, it is also processed to form a vector <em>q </em>∈ <em>R<sup>d</sup></em>. We now want to check if there is any duplicates or near duplicates of <em>q </em>in the database. Specifically, an image <em>o </em>is a near duplicate to <em>q </em>if <em>cos</em>(<em>θ</em>(<em>o,q</em>)) ≥ 0<em>.</em>9. We want to find any near duplicate with probability no less than 99%.

We now design an LSH scheme using <strong>SimHash </strong>to generate candidate near duplicates. Assume that for query <em>q</em>, there are 100 images that are near duplicate to <em>q</em>.

<ol>

 <li>Assume <em>k </em>= 5, how many tables does the LSH scheme require (i.e., <em>L</em>) to ensure that we can find any near duplicate with probability no less than 99%?</li>

 <li>Consider image <em>o </em>with <em>cos</em>(<em>θ</em>(<em>o,q</em>)) <em>&lt; </em>0<em>.</em>8, <em>k </em>= 5 and <em>L </em>= 10. What is the maximum value of the probability of <em>o </em>to become a false positive of query <em>q</em>?</li>

</ol>

You need to show the intermediate steps along with the final result to get full credit.




# Social characteristics of the Marvel Universe


```python
sc
```





        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-21-192.ec2.internal:4041">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        




```python
spark
```





            <div>
                <p><b>SparkSession - hive</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-21-192.ec2.internal:4041">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        
            </div>
        



## Read in the data.

Run the right cell depending on the platform being used.

For AWS EMR use:


```python
file = sc.textFile("s3://bigdatateaching/marvel/porgat.txt")
```

For Databricks on Azure use:


```python
file = sc.textFile("wasbs://marvel@bigdatateaching.blob.core.windows.net/porgat.txt")
```

## Clean The Data

The data file is in three parts, with the single file:

* Marvel Characters
* Publications
* Relationships between the two

We need to pre-process the file before we can use it. The following operations are all RDD operations.

Let's look at the file.

Count the number of records in the file.


```python
file.count()
```




    30520



Define a new RDD that removes the headers from the file. The headers are lines that begin with a star.


```python
noHeaders = file.filter(lambda x: len(x)>0 and x[0]!='*')
```

Look at the first 5 records of noHeaders


```python
noHeaders.take(20)
```




    [u'1 "24-HOUR MAN/EMMANUEL"',
     u'2 "3-D MAN/CHARLES CHAN"',
     u'3 "4-D MAN/MERCURIO"',
     u'4 "8-BALL/"',
     u'5 "A"',
     u'6 "A\'YIN"',
     u'7 "ABBOTT, JACK"',
     u'8 "ABCISSA"',
     u'9 "ABEL"',
     u'10 "ABOMINATION/EMIL BLO"',
     u'11 "ABOMINATION | MUTANT"',
     u'12 "ABOMINATRIX"',
     u'13 "ABRAXAS"',
     u'14 "ADAM 3,031"',
     u'15 "ABSALOM"',
     u'16 "ABSORBING MAN/CARL C"',
     u'17 "ABSORBING MAN | MUTA"',
     u'18 "ACBA"',
     u'19 "ACHEBE, REVEREND DOC"',
     u'20 "ACHILLES"']



Extract a pair from each line:  the leading integer and a string for the rest of the line


```python
paired = noHeaders.map(lambda l:  l.partition(' ')).filter(lambda t:  len(t)==3 and len(t[0])>0 and len(t[2])>0).map(lambda t: (int(t[0]), t[2]))
```


```python
paired.take(10)
```




    [(1, u'"24-HOUR MAN/EMMANUEL"'),
     (2, u'"3-D MAN/CHARLES CHAN"'),
     (3, u'"4-D MAN/MERCURIO"'),
     (4, u'"8-BALL/"'),
     (5, u'"A"'),
     (6, u'"A\'YIN"'),
     (7, u'"ABBOTT, JACK"'),
     (8, u'"ABCISSA"'),
     (9, u'"ABEL"'),
     (10, u'"ABOMINATION/EMIL BLO"')]




```python
p = paired.collect()
```


```python
p[30000]
```




    (6192, u'17877 19306 13619')



Filter relationships as they do not start with quotes, then split the integer list


```python
scatteredRelationships = paired.filter(lambda (charId, text):  text[0]!='"').map(lambda (charId, text): (charId, [int(x) for x in text.split(' ')]))
```

Relationships for the same character id sometime spans more than a line in the file, so let's group them together


```python
scatteredRelationships.take(10)
```




    [(1, [6487]),
     (2, [6488, 6489, 6490, 6491, 6492, 6493, 6494, 6495, 6496]),
     (3, [6497, 6498, 6499, 6500, 6501, 6502, 6503, 6504, 6505]),
     (4, [6506, 6507, 6508]),
     (5, [6509, 6510, 6511]),
     (6, [6512, 6513, 6514, 6515]),
     (7, [6516]),
     (8, [6517, 6518]),
     (9, [6519, 6520]),
     (10,
      [6521,
       6522,
       6523,
       6524,
       6525,
       6526,
       6527,
       6528,
       6529,
       6530,
       6531,
       6532,
       6533,
       6534,
       6535])]




```python
relationships = scatteredRelationships.reduceByKey(lambda pubList1, pubList2: pubList1 + pubList2)
```

Filter non-relationships as they start with quotes ; remove the quotes


```python
nonRelationships = paired.filter(lambda (index, text):  text[0]=='"').map(lambda (index, text):  (index, text[1:-1].strip()))
```

Characters stop at a certain line (part of the initial header ; we hardcode it here)


```python
characters = nonRelationships.filter(lambda (charId, name): charId<=6486)
```

Publications starts after the characters


```python
publications = nonRelationships.filter(lambda (charId, name): charId>6486)
```

The following cells begin to use SparkSQL. 

Spark SQL works with Data Frames which are a kind of “structured” RDD or an “RDD with schema”.

The integration between the two works by creating a RDD of Row (a type from pyspark.sql) and then creating a Data Frame from it.

The Data Frames can then be registered as views.  It is those views we’ll query using Spark SQL.


```python
from pyspark.sql import Row
```

Let's create dataframes out of the RDDs and register them as temporary views for SQL to use



```python
#  Relationships has a list as a component, let's flat that
flatRelationships = relationships.flatMap(lambda (charId, pubList):  [(charId, pubId) for pubId in pubList])
```


```python
#  Let's map the relationships to an RDD of rows in order to create a data frame out of it
relationshipsDf = spark.createDataFrame(flatRelationships.map(lambda t: Row(charId=t[0], pubId=t[1])))

```


```python
#  Register relationships as a temporary view
relationshipsDf.createOrReplaceTempView("relationships")

```


```python
spark.sql("select count(*) from relationships").collect()
```




    [Row(count(1)=96662)]




```python
df = spark.sql("select count(*) from relationships")
```


```python
df
```




    DataFrame[count(1): bigint]




```python
df_python = spark.sql("select count(*) from relationships").collect()
```


```python
df_python
```




    [Row(count(1)=96662)]




```python
relationshipsDf.count()
```




    96662




```python
spark.sql("select * from relationships limit 10").show()
```

    +------+-----+
    |charId|pubId|
    +------+-----+
    |     2| 6488|
    |     2| 6489|
    |     2| 6490|
    |     2| 6491|
    |     2| 6492|
    |     2| 6493|
    |     2| 6494|
    |     2| 6495|
    |     2| 6496|
    |     4| 6506|
    +------+-----+
    



```python
r2 = spark.read.csv("s3://bigdatateaching/marvel/relationship",header=True)
```


```python
r2.show()
```

    +------+-----+
    |charId|pubId|
    +------+-----+
    |     2| 6488|
    |     2| 6489|
    |     2| 6490|
    |     2| 6491|
    |     2| 6492|
    |     2| 6493|
    |     2| 6494|
    |     2| 6495|
    |     2| 6496|
    |     4| 6506|
    |     4| 6507|
    |     4| 6508|
    |     6| 6512|
    |     6| 6513|
    |     6| 6514|
    |     6| 6515|
    |     8| 6517|
    |     8| 6518|
    |    10| 6521|
    |    10| 6522|
    +------+-----+
    only showing top 20 rows
    



```python
#  Let's do the same for characters
charactersDf = spark.createDataFrame(characters.map(lambda t:  Row(charId=t[0], name=t[1])))
charactersDf.createOrReplaceTempView("characters")

```


```python
#  and for publications
publicationsDf = spark.createDataFrame(publications.map(lambda t:  Row(pubId=t[0], name=t[1])))
publicationsDf.createOrReplaceTempView("publications")
```


```python
relationshipsDf.show(10)
```

    +------+-----+
    |charId|pubId|
    +------+-----+
    |     2| 6488|
    |     2| 6489|
    |     2| 6490|
    |     2| 6491|
    |     2| 6492|
    |     2| 6493|
    |     2| 6494|
    |     2| 6495|
    |     2| 6496|
    |     4| 6506|
    +------+-----+
    only showing top 10 rows
    


The following cell is the standard way of running a SQL query on Spark. This query ranks Marvel characters in duo in order of join-appearances in publications. 


```python
df1 = spark.sql("""SELECT c1.name AS name1, c2.name AS name2, sub.charId1, sub.charId2, sub.pubCount
FROM
(
  SELECT r1.charId AS charId1, r2.charId AS charId2, COUNT(r1.pubId, r2.pubId) AS pubCount
  FROM relationships AS r1
  CROSS JOIN relationships AS r2
  WHERE r1.charId < r2.charId
  AND r1.pubId=r2.pubId
  GROUP BY r1.charId, r2.charId
) AS sub
INNER JOIN characters c1 ON c1.charId=sub.charId1
INNER JOIN characters c2 ON c2.charId=sub.charId2
ORDER BY sub.pubCount DESC
LIMIT 10""").cache()

```


```python
df1.take(10)
```




    [Row(name1=u'HUMAN TORCH/JOHNNY S', name2=u'THING/BENJAMIN J. GR', charId1=2557, charId2=5716, pubCount=744),
     Row(name1=u'HUMAN TORCH/JOHNNY S', name2=u'MR. FANTASTIC/REED R', charId1=2557, charId2=3805, pubCount=713),
     Row(name1=u'MR. FANTASTIC/REED R', name2=u'THING/BENJAMIN J. GR', charId1=3805, charId2=5716, pubCount=708),
     Row(name1=u'INVISIBLE WOMAN/SUE', name2=u'MR. FANTASTIC/REED R', charId1=2650, charId2=3805, pubCount=701),
     Row(name1=u'HUMAN TORCH/JOHNNY S', name2=u'INVISIBLE WOMAN/SUE', charId1=2557, charId2=2650, pubCount=694),
     Row(name1=u'INVISIBLE WOMAN/SUE', name2=u'THING/BENJAMIN J. GR', charId1=2650, charId2=5716, pubCount=668),
     Row(name1=u'SPIDER-MAN/PETER PAR', name2=u'WATSON-PARKER, MARY', charId1=5306, charId2=6166, pubCount=616),
     Row(name1=u'JAMESON, J. JONAH', name2=u'SPIDER-MAN/PETER PAR', charId1=2959, charId2=5306, pubCount=526),
     Row(name1=u'CAPTAIN AMERICA', name2=u'IRON MAN/TONY STARK', charId1=859, charId2=2664, pubCount=446),
     Row(name1=u'SCARLET WITCH/WANDA', name2=u'VISION', charId1=4898, charId2=6066, pubCount=422)]




```python
df2 = spark.sql("""
SELECT c1.name AS name1, c2.name AS name2, c3.name AS name3, sub.charId1, sub.charId2, sub.charId3, sub.pubCount
FROM
(
  SELECT r1.charId AS charId1, r2.charId AS charId2, r3.charId AS charId3, COUNT(r1.pubId, r2.pubId, r3.pubId) AS pubCount
  FROM relationships AS r1
  CROSS JOIN relationships AS r2
  CROSS JOIN relationships AS r3
  WHERE r1.charId < r2.charId
  AND r2.charId < r3.charId
  AND r1.pubId=r2.pubId
  AND r2.pubId=r3.pubId
  GROUP BY r1.charId, r2.charId, r3.charId
) AS sub
INNER JOIN characters c1 ON c1.charId=sub.charId1
INNER JOIN characters c2 ON c2.charId=sub.charId2
INNER JOIN characters c3 ON c3.charId=sub.charId3
ORDER BY sub.pubCount DESC
LIMIT 10
""").cache()

```


```python
df2.show(10)
```

    +--------------------+--------------------+--------------------+-------+-------+-------+--------+
    |               name1|               name2|               name3|charId1|charId2|charId3|pubCount|
    +--------------------+--------------------+--------------------+-------+-------+-------+--------+
    |HUMAN TORCH/JOHNNY S|MR. FANTASTIC/REED R|THING/BENJAMIN J. GR|   2557|   3805|   5716|     646|
    |HUMAN TORCH/JOHNNY S| INVISIBLE WOMAN/SUE|MR. FANTASTIC/REED R|   2557|   2650|   3805|     640|
    |HUMAN TORCH/JOHNNY S| INVISIBLE WOMAN/SUE|THING/BENJAMIN J. GR|   2557|   2650|   5716|     632|
    | INVISIBLE WOMAN/SUE|MR. FANTASTIC/REED R|THING/BENJAMIN J. GR|   2650|   3805|   5716|     612|
    |COLOSSUS II/PETER RA|STORM/ORORO MUNROE S|     WOLVERINE/LOGAN|   1127|   5467|   6306|     297|
    |     CAPTAIN AMERICA| SCARLET WITCH/WANDA|              VISION|    859|   4898|   6066|     286|
    |CYCLOPS/SCOTT SUMMER| ICEMAN/ROBERT BOBBY|MARVEL GIRL/JEAN GRE|   1289|   2603|   3495|     278|
    |BEAST/HENRY &HANK& P|CYCLOPS/SCOTT SUMMER|MARVEL GIRL/JEAN GRE|    403|   1289|   3495|     277|
    |     CAPTAIN AMERICA| IRON MAN/TONY STARK|THOR/DR. DONALD BLAK|    859|   2664|   5736|     273|
    |ANGEL/WARREN KENNETH|BEAST/HENRY &HANK& P| ICEMAN/ROBERT BOBBY|    133|    403|   2603|     272|
    +--------------------+--------------------+--------------------+-------+-------+-------+--------+
    



```python
sc.stop()
```

This lab was adapted from [https://vincentlauzon.com/2018/01/24/azure-databricks-spark-sql-data-frames/](https://vincentlauzon.com/2018/01/24/azure-databricks-spark-sql-data-frames/)

Saving a DataFrame to a csv
```
publicationsDf.write\
    .format("com.databricks.spark.csv")\
    .option("header", "true")\
    .save("s3://bigdatateaching/marvel/publication")
```

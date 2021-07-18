// input data

[root@nvmbdprp009117 data]# cat rws.csv
UserId,episodeId,timestamp,duration
52627f47-f078-4168-a64d-b43b0278346f,889090,1594302606,400
52627f47-f078-4168-a64d-b43b0278346f,100370,1593697806,30
52627f47-f078-4168-a64d-b43b0278346f,300540,1593611406,120
52627f47-f078-4168-a64d-b43b0278346f,800910,1589550606,3600
52627f47-f078-4168-a64d-b43b0278346f,687100,1589809806,1680
52627f47-f078-4168-a64d-b43b0278346f,108456,1590933006,420
52627f47-f078-4168-a64d-b43b0278346f,108756,1591105806,670
52627f47-f078-4168-a64d-b43b0278346f,300120,1591624206,1540
52627f47-f078-4168-a64d-b43b0278346f,891091,1592056206,1266
52627f47-f078-4168-a64d-b43b0278346f,900123,1591710606,1080



// Code written in spark-shell


// importing the input data to spark dataframe
scala> val a = spark.read.format("csv").option("header", true).option("inferSchema", true).load("file:///home/hdfs/sankar/data/rws.csv").select($"UserId",$"duration",$"timestamp")
a: org.apache.spark.sql.DataFrame = [UserId: string, duration: double ... 1 more field]

scala> a.show()
+--------------------+--------+----------+
|              UserId|duration| timestamp|
+--------------------+--------+----------+
|52627f47-f078-416...|   400.0|1594302606|
|52627f47-f078-416...|    30.0|1593697806|
|52627f47-f078-416...|   120.0|1593611406|
|52627f47-f078-416...|  3600.0|1589550606|
|52627f47-f078-416...|  1680.0|1589809806|
|52627f47-f078-416...|   420.0|1590933006|
|52627f47-f078-416...|   670.0|1591105806|
|52627f47-f078-416...|  1540.0|1591624206|
|52627f47-f078-416...|  1266.0|1592056206|
|52627f47-f078-416...|  1080.0|1591710606|
+--------------------+--------+----------+


// converting the unix time to "MMM/YYYY"
scala> val b = a.withColumn("date", date_format(from_unixtime(col("timestamp")), "MMM/YYYY")).drop(col("timestamp"))
b: org.apache.spark.sql.DataFrame = [UserId: string, duration: double ... 1 more field]

scala> b.show()
+--------------------+--------+--------+
|              UserId|duration|    date|
+--------------------+--------+--------+
|52627f47-f078-416...|   400.0|Jul/2020|
|52627f47-f078-416...|    30.0|Jul/2020|
|52627f47-f078-416...|   120.0|Jul/2020|
|52627f47-f078-416...|  3600.0|May/2020|
|52627f47-f078-416...|  1680.0|May/2020|
|52627f47-f078-416...|   420.0|May/2020|
|52627f47-f078-416...|   670.0|Jun/2020|
|52627f47-f078-416...|  1540.0|Jun/2020|
|52627f47-f078-416...|  1266.0|Jun/2020|
|52627f47-f078-416...|  1080.0|Jun/2020|
+--------------------+--------+--------+


// summing hours grouped by months and year 
scala> val d = b.groupBy(col("userId"), col("date")).agg(sum(col("duration")).as("grouped_duration"))
d: org.apache.spark.sql.DataFrame = [userId: string, date: string ... 1 more field]

scala> d.show()
+--------------------+--------+----------------+
|              userId|    date|grouped_duration|
+--------------------+--------+----------------+
|52627f47-f078-416...|Jul/2020|           550.0|
|52627f47-f078-416...|May/2020|          5700.0|
|52627f47-f078-416...|Jun/2020|          4556.0|
+--------------------+--------+----------------+


// segregating MMM/YYYY to two columns - Month and Year
scala> val e = d.withColumn("_temp", split(col("date"),"/")).withColumn("Month", col("_temp").getItem(0)).withColumn("Year", col("_temp").getItem(1)).drop("_temp","userId","date")
e: org.apache.spark.sql.DataFrame = [grouped_duration: double, Month: string ... 1 more field]

scala> e.show()
+----------------+-----+----+
|grouped_duration|Month|Year|
+----------------+-----+----+
|           550.0|  Jul|2020|
|          5700.0|  May|2020|
|          4556.0|  Jun|2020|
+----------------+-----+----+



scala> val f = e.select(col("Year"),concat(col("Month"),lit(":"),col("grouped_duration"))).withColumnRenamed("concat(Month, :, grouped_duration)", "total_hours")
f: org.apache.spark.sql.DataFrame = [Year: string, total_hours: string]

scala> f.show()
+----+-----------+
|Year|total_hours|
+----+-----------+
|2020|  Jul:550.0|
|2020| May:5700.0|
|2020| Jun:4556.0|
+----+-----------+

scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._


scala> val g = f.groupBy(col("Year")).agg(collect_set(col("total_hours"))).withColumnRenamed("collect_set(total_hours)","json_data").select(col("Year"),col("json_data").cast(StringType))
g: org.apache.spark.sql.DataFrame = [Year: string, json_data: string]


scala> g.show(false)
+----+-----------------------------------+
|Year|json_data                          |
+----+-----------------------------------+
|2020|[Jun:4556.0, May:5700.0, Jul:550.0]|
+----+-----------------------------------+


scala> val h = g.select(concat(lit("{"),col("Year"),lit(":{"),col("json_data"),lit("}")))
h: org.apache.spark.sql.DataFrame = [concat({, Year, :{, json_data, }): string]


scala> h.show(false)
+-------------------------------------------+
|concat({, Year, :{, json_data, })          |
+-------------------------------------------+
|{2020:{[Jun:4556.0, May:5700.0, Jul:550.0]}|
+-------------------------------------------+



import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._


// load file and remove header
val data = sc.textFile("hdfs:///user/hashaalb/joined_table.csv")
val header = data.first
val rows = data.filter(l => l != header)

// define case class
case class CC1(Borough: String, District: String, Population: Int, RefuseTons: Double, PaperTons: Double, MGPTons: Double, D_rate: Double, CP_rate: Double, CMGP_rate: Double, C_rate: Double, RefuseRate: Double, PaperRate: Double, MGPRate: Double)

// comma separator split
val allSplit = rows.map(line => line.split(","))

// map parts to case class to create an RDD of 
val allData = allSplit.map( p => CC1( p(0).toString, p(1).toString, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim.toDouble, p(9).trim.toDouble, p(10).trim.toDouble, p(11).trim.toDouble, p(12).trim.toDouble))

// convert rdd to dataframe
val allDF = allData.toDF()

//Display the data
allDF.show()

// convert back to rdd and cache the data
val rowsRDD = allDF.rdd.map(r => (r.getString(0), r.getString(1), r.getInt(2), r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getDouble(11), r.getDouble(12)))

rowsRDD.cache()


// convert data to RDD which will be passed to KMeans and cache the data. We are passing in (RefuseTons, PaperTons, MGPTons, D_rate, CP_rate) to KMeans. These are the attributes we want to use to assign the instance to a cluster
 
val vectors = allDF.rdd.map(r => Vectors.dense(r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6), r.getDouble(7)))

vectors.cache()

//KMeans model with 5 clusters and 20 iterations

val kMeansModel = KMeans.train(vectors, 5, 20)


//Print the center of each cluster
kMeansModel.clusterCenters.foreach(println)

// Get the prediction from the model with the District so we can link them back to other information

val predictions = rowsRDD.map{r => (r._2, kMeansModel.predict(Vectors.dense(r._4,r._5,r._6,r._7,r._8) ))}

//converting the RDD back to Dataframe
val predDF = predictions.toDF("District","CLUSTER")

//display it
predDF.show()


//printing 59 lines (all of the lines)
predDF.take(59).foreach(println)


//methods tried to join predDF with allDF:
// join the dataframes on ID (spark 1.4.1)
//val t = allDF.join(predDF, "District")
//t.show()

//Method to save predDF to HDFS and later copyToLocal to join it outside of spark because it is hanging
//predDF.write.format("com.databricks.spark.csv").save("hdfs:///user/hashaalb/table")


//to exit spark scala shell
//System.exit(0)
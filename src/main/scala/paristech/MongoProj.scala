package paristech


import com.mongodb.client.{MongoClient, MongoDatabase}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.bson.Document
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

import org.mongodb.scala._

object MongoProj {

  def main(args: Array[String]): Unit = {


    //CF conexion String d'ATLAS
    val uri: String = "mongodb+srv://<username>:<password>@cluster0.ldu6v.mongodb.net/<dbname>?retryWrites=true&w=majority"
    //System.setProperty("org.mongodb.async.type", "netty")
    //val client: MongoClient = MongoClient(uri)
    //val db: MongoDatabase = client.getDatabase("test")


    // Des réglages optionnels du job spark.

    //POINT sur spark.master ->local
    //https://spark.apache.org/docs/latest/cluster-overview.html#cluster-manager-types
    //Driver program	: The process running the main() function of the application and creating the SparkContext
    //Cluster manager	: An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)
    //Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).
    //the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications.
    //donc Spark driver <-> Spark manager en STANDALONE
    //Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application.
    //Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors.
    //Finally, SparkContext sends tasks to the executors to run.

    val sparkConf = new SparkConf().setAll(Map(
      "spark.master" -> "local",
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.mongodb.input.uri" -> "mongodb+srv://admin:admin@cluster0.ldu6v.mongodb.net/DBtest.COLLtest",
      "spark.mongodb.output.uri" -> "mongodb+srv://admin:admin@cluster0.ldu6v.mongodb.net/DBtest.COLLtest"
    ))

    //Affiche recap conf avec versions dependencies
    sparkConf.getAll.toString

    val spark = SparkSession.builder()
      .appName("MongoSparkConnectorIntro")
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // BUG SCHEMA : case class event + codec?
    //https://docs.mongodb.com/spark-connector/current/scala/datasets-and-sql
    //val df = MongoSpark.load[PreprocessorFuret.Event](spark)
    //df.printSchema()  //parait pourtant OK

    //val personCodecProvider = Macros.createCodecProviderIgnoreNone[PreprocessorFuret.Event]()
    //val codecRegistry = fromRegistries( fromProviders(personCodecProvider), DEFAULT_CODEC_REGISTRY )
    //val df = MongoSpark.load[PreprocessorFuret.Event](spark).toDF();

    val df = MongoSpark.load(spark)

    df.printSchema()
    df.show(2, truncate = 62, vertical = true)
    df.filter(df("SOURCEURL").like("%covid%")).count
    df.filter(df("SOURCEURL").like("%covid%")).select(df("GLOBALEVENTID"),df("SOURCEURL")).show(2)
    println(s"Nombre de lignes : ${df.count}")
    println(s"Nombre de colonnes : ${df.columns.length}")

    //TESTS bug
    //
    val readConfig = ReadConfig(Map("collection" -> "COLLtest", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(spark)))
    val customRdd = MongoSpark.load(spark, readConfig)
    customRdd
    customRdd.filter(df("SOURCEURL").like("%COVID%")).toDF().count //NE PASSE PAS ANALYSIS FAILURE
    //alors que ca ca passe, or les deux sont des dataframes, on a juste voulu une config manuelle write car deux collections à gérer
    //SparkSQL NOK
    //df.createOrReplaceTempView("vue")
    //val centenarians = spark.sql("SELECT GLOBALEVENTID FROM vue WHERE Actor2Geo_Type <1000")
    //centenarians.show()

    ///NOK
    //df.filter(df("Actor2Geo_Type")>1).show(2, truncate = 62, vertical = true)

    //conv dataset NOK
    //val ds = df.as[Event]
    //ds.filter(ds("Actor2Geo_Type")>1).show(2, truncate = 62, vertical = true)



    //PIPELINE mongo

    // 2 solutions
    // avec sc en param, on obtient un MongoRDD, or si on reste sur la ss spark en param, on a un DataFrame

    //SOL1
    //val rdd = MongoSpark.load(sc)
    //val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { EventRootCode  : { $gt : 3 } } }")))
    //println(aggregatedRdd.count)

    //SOL2
    // cf https://docs.mongodb.com/spark-connector/current/scala/datasets-and-sql
    //When using filters with DataFrames or Spark SQL, the underlying Mongo Connector code constructs an aggregation pipeline to filter the data in MongoDB before sending it to Spark.
    // Donc avantages pipeline acquis en codant comme ci dessous on dirait
    //TODO modif schema pour caster tout, cf le ppt de presentation du proj


  }


  def doStuff(a: Int, b: Int): Int = {
    val sum = a + b
    val doubled = sum * 2
    doubled
  }


  case class Event(GLOBALEVENTID: BigInt,
                   SQLDATE: Int,
                   MonthYear: Int,
                   Year: Int,
                   FractionDate: Double,
                   Actor1Code: String,
                   Actor1Name: String,
                   Actor1CountryCode: String,
                   Actor1KnownGroupCode: String,
                   Actor1EthnicCode: String,
                   Actor1Religion1Code: String,
                   Actor1Religion2Code: String,
                   Actor1Type1Code: String,
                   Actor1Type2Code: String,
                   Actor1Type3Code: String,
                   Actor2Code: String,
                   Actor2Name: String,
                   Actor2CountryCode: String,
                   Actor2KnownGroupCode: String,
                   Actor2EthnicCode: String,
                   Actor2Religion1Code: String,
                   Actor2Religion2Code: String,
                   Actor2Type1Code: String,
                   Actor2Type2Code: String,
                   Actor2Type3Code: String,
                   IsRootEvent: Int,
                   EventCode: String,
                   EventBaseCode: String,
                   EventRootCode: String,
                   QuadClass: Int,
                   GoldsteinScale: Double,
                   NumMentions: Int,
                   NumSources: Int,
                   NumArticles: Int,
                   AvgTone: Double,
                   Actor1Geo_Type: Int,
                   Actor1Geo_FullName: String,
                   Actor1Geo_CountryCode: String,
                   Actor1Geo_ADM1Code: String,
                   Actor1Geo_ADM2Code: String,
                   Actor1Geo_Lat: Double,
                   Actor1Geo_Long: Double,
                   Actor1Geo_FeatureID: String,
                   Actor2Geo_Type: Int,
                   Actor2Geo_FullName: String,
                   Actor2Geo_CountryCode: String,
                   Actor2Geo_ADM1Code: String,
                   Actor2Geo_ADM2Code: String,
                   Actor2Geo_Lat: Double,
                   Actor2Geo_Long: Double,
                   Actor2Geo_FeatureID: String,
                   ActionGeo_Type: Int,
                   ActionGeo_FullName: String,
                   ActionGeo_CountryCode: String,
                   ActionGeo_ADM1Code: String,
                   ActionGeo_ADM2Code: String,
                   ActionGeo_Lat: Double,
                   ActionGeo_Long: Double,
                   ActionGeo_FeatureID: String,
                   DATEADDED: BigInt,
                   SOURCEURL: String)

}
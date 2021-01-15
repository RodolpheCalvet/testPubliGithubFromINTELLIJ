package paristech

import com.amazonaws.AmazonWebServiceClient
import com.amazonaws.auth.{BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

// Imports
import sys.process._
import java.net.URL
import java.io.File
import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.net.HttpURLConnection
import org.apache.spark.sql.functions._
//import sqlContext.implicits._
import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.sql.SQLContext
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.sql.types.IntegerType
import com.amazonaws.regions.{Region, Regions}
//import com.amazonaws.services.lambda.runtime.events.S3Event
//import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
//import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
//import com.amazonaws.client.builder.AwsClientBuilder




object AWSProj {

  def main(args: Array[String]): Unit = {


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
    ))

    //Affiche recap conf avec versions dependencies
    sparkConf.getAll.toString

    val spark = SparkSession.builder()
      .appName("AWSProj")
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    def fileDownloader(urlOfFileToDownload: String, fileName: String) = {
      val url = new URL(urlOfFileToDownload)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.connect()

      if (connection.getResponseCode >= 400)
        println("error")
      else
        url #> new File(fileName) !!
    }

    // Download locally the list of URL
    // A VOIR SI vers spark master plutot cf andrei
    //fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", "/tmp/masterfilelist.txt") // save the list file to the Spark Master
    //fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt", "/tmp/masterfilelist_translation.txt")

    //arn:aws:s3:::furets
    val AWS_ID = "ASIAQFYNH7PY2XSMFTXO"
    val AWS_KEY = "VHLxovmm5ORlJo4Cqv9t/kwhUYn36Bet2iSMAa6+" //seccret one
    val AWS_SESSION_TOKEN =
    "FwoGZXIvYXdzEE4aDG4Ejblc6qoGxfVmnSLQAc4h0uLa7DzeiqIh1Oc67P6o87KmnjAReu3QDT+6xvGf1ZoeEJidMQSNC7aElElf+Z8DT6qhngEd83cITzdVmDgfym49S+XI4czQg5LYlDauk+j7pVJNcj/Fy22KhAiF2mFeOxUD05gxfcnVjFwg3fUTBAkX5C2n2cclOdxqFwGQpawhF4ByagdmVPWgT7CmO8uAH6iqHIDAe0CylUTw1O16svxtH6ekab2JrrrDO77meOVvevx+boPysP7caA9G3XGkdrh9dB8SVfoPnHd88boohZOGgAYyLQxI6NzI4PoR+cNwaAKgafkKOM4By4tJCP2Lq7GS6U/JbEGzlzOoPXP5UsbMJw=="
    // la classe AmazonS3Client n'est pas serializable
    // on rajoute l'annotation @transient pour dire a Spark de ne pas essayer de serialiser cette
    @transient val awsClient = new AmazonS3Client(new BasicSessionCredentials(AWS_ID, AWS_KEY, AWS_SESSION_TOKEN))

    //furets OU rod-gdelt
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) //(1) mettre votre ID du fichier credentials.csv
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) //(2) mettre votre secret du fichier credentials.csv
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", AWS_SESSION_TOKEN) //(3) 15 par default !!!

    //  A REFAIRE POUR REFRESH dernières actus : la masterList
    //awsClient.putObject("rod-gdelt", "masterfilelist.txt", new File("/tmp/masterfilelist.txt"))
    //awsClient.putObject("rod-gdelt", "masterfilelist_translation.txt", new File("/tmp/masterfilelist_translation.txt"))

    val sqlContext = spark.sqlContext

    val racineTemps : String = "2021010100"

// CI DESSOUS A FAIRE JUSTE UNE FOIS ::: ECRITURE SUR S3 ::: des fichiers ici .contains("/2021010100")). d'abord downloadés sur tmp/ local
//  puis uploadés S3 avec suprrssion dossiers en local enfin
//    sqlContext.read.
//      option("delimiter"," ").
//      option("infer_schema","true").
//      //csv("https://rod-gdelt.s3.us-east-1.amazonaws.com/masterfilelist-translation.txt").
//      csv("/tmp/masterfilelist.txt").
//      withColumnRenamed("_c2","url").
//      filter(col("url").contains("/2021010100")).
//      repartition(200).
//      foreach( r=> {                               //  J  EN SUIS LA :
//      val URL = r.getAs[String](2)
//      val fileName = r.getAs[String](2).split("gdeltv2/").last
//      val dir = "/cal/homes/rcalvet/furets/"
//      val localFileName = dir + fileName
//      fileDownloader(URL,  localFileName)
//      val localFile = new File(localFileName)
//      @transient val awsClient = new AmazonS3Client(new BasicSessionCredentials(AWS_ID, AWS_KEY, AWS_SESSION_TOKEN))
//      val pto = awsClient.putObject("rod-gdelt", fileName, localFile ).getVersionId
//      val res = localFile.delete()
//    })

    //https://rod-gdelt.s3.amazonaws.com/20210101000000.export.CSV.zip
    //val eventsRDD = spark.sparkContext.binaryFiles("https://rod-gdelt.s3.us-east-1.amazonaws.com/20210101000000.export.CSV.zip").
    val eventsRDD = spark.sparkContext.binaryFiles("s3a://rod-gdelt/20210101[0-9]*.export.CSV.zip", 100).
      //"s3a://rod-gdelt", 100).
      //20210101*.mentions.CSV.zip",100).
      flatMap {  // decompresser les fichiers
        case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile(_ != null).
            flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }
    val cachedEvents = eventsRDD.cache // RDD

    cachedEvents.take(1)

    def doStuff(a: Int, b: Int): Int = {
      val sum = a + b
      val doubled = sum * 2
      doubled
    }
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
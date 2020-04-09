package ca.usask.hdf5

import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

object Spark {

  val BLOCK_SIZE: Long = 128 * 1024 * 1024

  val APP_NAME = "P2IRC"
  val MASTER = "local[*]"
  val DISABLE_SPARK_LOGS = true
  val ES_HOST = "localhost"

  private lazy val logger = java.util.logging.Logger.getLogger(APP_NAME)

  lazy val es: RestHighLevelClient = {
    // For filters this is no good, since we need the master to have the environment variable so sometimes hard coding
    // is necessary
    //val hosts = sys.env.getOrElse("ELASTICSEARCH_HOSTS", "192.168.1.30 192.168.1.31").split(' ').filter(_.length > 0).map((hostName: String) => {
    val hosts = sys.env.getOrElse("ELASTICSEARCH_HOSTS", "localhost").split(' ').filter(_.length > 0).map((hostName: String) => {
      new HttpHost(hostName, 9200, "http")
    })
    if (hosts.length == 0) {
      logger.info("Elasticsearch disabled.")
      null
    } else {
      logger.info("Elasticsearch host configured for: " + hosts.map(_.getHostName).mkString(" "))
      new RestHighLevelClient(
        RestClient.builder(
          hosts: _*)
          .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder =
              requestConfigBuilder.setSocketTimeout(60000).setConnectTimeout(10000).setConnectionRequestTimeout(30000)
          }))
    }
  }

  lazy val sc: SparkContext = {
    System.setProperty("java.library.path", "lib/")

    logger
      .info(
        s"\n" +
          s"${"*" * 80}\n" +
          s"${"*" * 80}\n" +
          s"**${" " * 76}**\n" +
          s"**${" " * (38 - Math.ceil(APP_NAME.length / 2.0).toInt)}$APP_NAME${" " * (38 - APP_NAME.length / 2)}**\n" +
          s"**${" " * 76}**\n" +
          s"${"*" * 80}\n" +
          s"${"*" * 80}\n"
      )

    if (DISABLE_SPARK_LOGS) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }

    val conf = new SparkConf()
    conf.set("spark.ui.showConsoleProgress", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if (!conf.contains("spark.app.name")) {
      conf.setAppName(APP_NAME)
    }

    if (!conf.contains("spark.master")) {
      conf.setMaster(MASTER)
    }

    val ctx = new SparkContext(conf)

    logger.info(s"Master is ${ctx.master}")
    logger.info(s"Default parallelism = ${ctx.defaultParallelism}")

    ctx
  }

  def defaultParallelism: Int = sc.defaultParallelism
}

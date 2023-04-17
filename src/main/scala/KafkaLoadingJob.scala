import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Properties
import scala.sys.process._
import scala.language.postfixOps

object KafkaLoadingJob extends App with LazyLogging {

  val target_table = args(0)
  val bootstrap_servers = args(1)
  val kafka_login = args(2)
  val kafka_provider_path = args(3)
  val kafka_password_alias = args(4)
  val subscribe_topic = args(5)
  val group_id = args(6)
  val truststore_location = args(7)
  val truststore_type = args(8)
  val partition_attribute = args(9)
  val kafka_security_protocol = args(10)
  val kafka_sasl_mechanism = args(11)
  val starting_offsets = args(12)

  val database = target_table.split("\\.")(0)
  val table_name = target_table.split("\\.")(1)

  val dbHadoopConf = new Configuration()
  dbHadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, kafka_provider_path)


  val kafkaCredentials = new Properties()
  kafkaCredentials.put("user", kafka_login)
  val password = dbHadoopConf.getPassword(kafka_password_alias).mkString
  kafkaCredentials.put("password", password)

  val jaas_config = s"""org.apache.kafka.common.security.plain.PlainLoginModule required serviceName = "kafka" username="$kafka_login" password="$password";"""

  val sparkSession = SparkSession.builder()
    .appName("LoadingKafkaJob")
    .enableHiveSupport()
    .getOrCreate()

  import sparkSession.implicits._

  val inputStream = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", subscribe_topic)
    .option("startingOffsets", starting_offsets)
    .option("kafka.group.id", group_id)
    .option("failOnDataLoss", value = false)
    .option("kafka.security.protocol", kafka_security_protocol)
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.truststore.location", truststore_location)
    .option("kafka.ssl.truststore.type", truststore_type)
    .option("kafka.ssl.truststore.password", "STOREPASSW0RD")
    .option("kafka.ssl.endpoint.identification.algorithm", "")
    .load()

  val PATH_FILES = database match {
    case "d_sourcedata" => s"/dev/d_sourcedata/db/$table_name"
    case "t_sourcedata" => s"/test/t_sourcedata/db/$table_name"
    case "s_sourcedata" => s"/storage/s_sourcedata/db/$table_name"
    case _ => "Invalid schema type. Should be <d|t|s>_sourcedata"
  }
  val RAW_TABLE_PATH = PATH_FILES
  val PATH_INPUT_FILES = PATH_FILES + "_tmp/data"
  val PATH_CHECKPOINTS_FILES = PATH_FILES + "_tmp/checkpoints"

  s"hdfs dfs -rm ${PATH_INPUT_FILES}/*.parquet"!

  val preparedDS = inputStream.selectExpr("CAST(value AS STRING)").as[String]
  val rawData = preparedDS.filter($"value".isNotNull)
  val parsedData = rawData.select(col("value").as("message"))

  parsedData
    .repartition(1)
    .writeStream
    .format("parquet")
    .option("checkpointLocation", PATH_CHECKPOINTS_FILES)
    .option("path", PATH_INPUT_FILES)
    .trigger(Trigger.Once())
    .outputMode(OutputMode.Append())
    .start()

  sparkSession.streams.awaitAnyTermination()
}
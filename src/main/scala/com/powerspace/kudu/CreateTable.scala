package com.powerspace.kudu

import com.powerspace.kudu.converters.{AvroConverter, Converter, KuduColumnBuilder, SqlConverter}
import org.apache.kudu.{ColumnSchema, Schema}
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.apache.kudu.client.CreateTableOptions
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

case class Config(
                   tableName: String = "demo",
                   pkeys: List[String] = List("id"),
                   avroSchemaPath: Option[String] = None,
                   kuduServers: List[String] = List(),
                   sql: Option[String] = None,
                   compressed: Boolean = true,
                   replica: Int = 3,
                   buckets: Int = 32
                 )

object CreateTable extends App {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val listRead: scopt.Read[List[String]] =
    scopt.Read.reads(x => x.split(',').toList)

  val parser = new scopt.OptionParser[Config]("kudu-from-avro") {
    opt[String]('t', "table").required()
      .action((x, c) => c.copy(tableName = x))
      .text("Table to create in Kudu")

    opt[List[String]]('p', "primary_key").required()
      .action((x, c) => c.copy(pkeys = x))
      .text("Primary key column name in the Kudu table")

    opt[Int]('r', "replica")
      .action((x, c) => c.copy(replica = x))
      .text("Number of replicas (default: 3)")

    opt[Int]('b', "buckets")
      .action((x, c) => c.copy(replica = x))
      .text("Number of buckets (default: 32)")

    opt[String]('s', "avro_schema")
      .action((x, c) => c.copy(avroSchemaPath = Some(x)))
      .text(".avsc to read to create the table")

    opt[Boolean]('c', "compressed")
      .action((x, c) => c.copy(compressed = x))
      .text("Compress columns using LZ4")

    opt[List[String]]('k', "kudu_servers").required()
      .action((x, c) => c.copy(kuduServers = x))
      .text("Kudu master tablets")

    opt[String]('q', "sql")
      .action((x, c) => c.copy(sql = Some(x)))
      .text("Custom SQL creation to create columns from: \"id INTEGER, ts BIGINT, name STRING\"")

    checkConfig(c =>
      if (c.sql.isDefined && c.avroSchemaPath.isDefined) {
        failure("Can't use an SQL and an Avro schema at the same time")
      } else {
        success
      }
    )
  }
  parser.parse(args, Config()) match {
    case Some(config) => createTable(config)
    case None =>
  }

  def createTable(config: Config) = {
    logger.info(config.toString)

    val columns = buildKuduColumns(converter(config), config.pkeys, config.compressed)
    val options = buildKuduTableOptions(config)

    val newTableName = config.tableName

    logger.info(s"Creating table $newTableName...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    client.createTable(newTableName, new Schema(columns.asJava), options).join()
    logger.info(s"Table $newTableName created!")

  }

  def buildKuduTableOptions(config: Config): CreateTableOptions = {
    new CreateTableOptions()
      .addHashPartitions(config.pkeys.asJava, config.buckets)
      .setNumReplicas(config.replica)
  }

  def buildKuduColumns(converter: Converter, pkeys: List[String], compressed: Boolean): List[ColumnSchema] = {
    // we must order by "key" first for Kudu
    //implicit def orderingByName[A <: ColumnSchema]: Ordering[A] = Ordering.by(!_.isKey)
    val compression = if (compressed) CompressionAlgorithm.LZ4 else CompressionAlgorithm.NO_COMPRESSION

    converter.kuduColumns().map { case KuduColumnBuilder(name, builder) =>
      builder.compressionAlgorithm(compression).key(pkeys.contains(name)).build()
    }.sortWith(sortColumns(_,_)(pkeys))
  }

  def sortColumns(a: ColumnSchema, b: ColumnSchema)(pkeys: List[String]): Boolean = {
    if (pkeys.indexOf(a.getName) < 0) false
    else if (pkeys.indexOf(b.getName) < 0) true
    else pkeys.indexOf(a.getName) < pkeys.indexOf(b.getName)
  }

  private def converter(config: Config): Converter = {
    (config.avroSchemaPath.map(file => AvroConverter(Source.fromFile(file).mkString))
     orElse config.sql.map(SqlConverter(_, config.pkeys)))
      .getOrElse(???)
  }
}
